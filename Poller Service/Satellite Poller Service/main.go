package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"satellitepoller/client"
	"satellitepoller/domain"
	"satellitepoller/processing"
	"satellitepoller/storage"
)

const (
	DefaultSatelliteBaseURL       = "https://satellite.naufarrel.dev"
	DefaultReconnectDelay         = 5 * time.Second
	DefaultMaxLateness            = 30 * time.Minute
	DefaultSQLiteDBPath           = "/var/lib/satellite-poller/state.db"
	DefaultKafkaBrokers           = "kafka-aggregate:29092"
	DefaultKafkaTopicHazardIngest = "hazard-ingest"
	DefaultHealthPort             = "8085"
	defaultBatchSize              = 100
	defaultPollInterval           = time.Second
	startupTimeout                = 15 * time.Second
	shutdownTimeout               = 10 * time.Second
)

type Config struct {
	SatelliteBaseURL       string
	SatelliteGroupToken    string
	ReconnectDelay         time.Duration
	MaxLateness            time.Duration
	SQLiteDBPath           string
	KafkaBrokers           []string
	KafkaTopicHazardIngest string
	HealthPort             string
}

// OutboxRepository is the subset of storage.Repository used by the publisher.
type OutboxRepository interface {
	GetPendingOutboxEvents(ctx context.Context, limit int) ([]domain.OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, eventID string, publishedAt time.Time) error
	MarkOutboxFailed(ctx context.Context, eventID, cause string) error
}

// KafkaWriter makes the publisher testable without a real Kafka broker.
type KafkaWriter interface {
	WriteMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

// Encoder converts a durable outbox record into the Kafka wire payload.
// Implement a different Encoder if the Aggregator requires another wire format.
type Encoder interface {
	Encode(event domain.OutboxEvent) ([]byte, error)
}

func main() {
	if err := run(); err != nil {
		slog.Error("satellite poller stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	// Use the signal context for startup as well as normal operation.
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancelStartup := context.WithTimeout(rootCtx, startupTimeout)
	db, err := storage.Open(startupCtx, cfg.SQLiteDBPath)
	cancelStartup()
	if err != nil {
		return fmt.Errorf("open SQLite storage: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Warn("close SQLite database", "error", err)
		}
	}()

	repo, err := storage.NewRepository(db)
	if err != nil {
		return fmt.Errorf("create storage repository: %w", err)
	}

	feedClient, err := client.NewClient(cfg.SatelliteBaseURL, cfg.SatelliteGroupToken, nil)
	if err != nil {
		return fmt.Errorf("create satellite feed client: %w", err)
	}

	sessionCtx, cancelSession := context.WithTimeout(rootCtx, startupTimeout)
	session, err := feedClient.GetSession(sessionCtx)
	cancelSession()
	if err != nil {
		return fmt.Errorf("get satellite session: %w", err)
	}

	latestOffset, err := session.LatestOffset()
	if err != nil {
		return fmt.Errorf("read session latest_offset: %w", err)
	}

	logger.Info("satellite session verified",
		"latest_offset", latestOffset,
	)

	ingestionService, err := client.NewService(feedClient, repo, client.Options{
		ReconnectDelay:  cfg.ReconnectDelay,
		LatestOffset:    latestOffset,
		HasLatestOffset: true,
		Logger:          logger,
	})
	if err != nil {
		return fmt.Errorf("create ingestion service: %w", err)
	}

	processor, err := processing.NewProcessor(
		repo,
		processing.Options{
			MaxLateness: cfg.MaxLateness,
			Logger:      logger,
		},
	)
	if err != nil {
		return fmt.Errorf("create processing service: %w", err)
	}
	kafkaPublisher, err := NewKafkaPublisher(
		cfg.KafkaBrokers,
		cfg.KafkaTopicHazardIngest,
		repo,
		Options{
			Logger:  logger,
			Encoder: HazardProtoJSONEncoder{},
		},
	)
	if err != nil {
		return fmt.Errorf("create Kafka publisher: %w", err)
	}
	defer func() {
		if err := kafkaPublisher.Close(); err != nil {
			logger.Warn("close Kafka writer", "error", err)
		}
	}()

	healthServer := newHealthServer(cfg.HealthPort, db)
	workerCtx, cancelWorkers := context.WithCancel(rootCtx)
	defer cancelWorkers()

	// Three independent loops share the service-owned SQLite repository:
	// - ingestion persists raw SSE events
	// - processing creates outbox records
	// - publisher sends those records to Kafka
	type component struct {
		name string
		run  func(context.Context) error
	}
	components := []component{
		{name: "ingestion", run: ingestionService.Run},
		{name: "processing", run: processor.Run},
		{name: "kafka-publisher", run: kafkaPublisher.Run},
	}

	errCh := make(chan error, len(components)+1)
	var wg sync.WaitGroup
	for _, item := range components {
		item := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := item.run(workerCtx); err != nil {
				if workerCtx.Err() == nil {
					errCh <- fmt.Errorf("%s worker: %w", item.name, err)
				}
				return
			}
			if workerCtx.Err() == nil {
				errCh <- fmt.Errorf("%s worker stopped unexpectedly", item.name)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("health server listening", "address", healthServer.Addr)
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("health server: %w", err)
		}
	}()

	logger.Info("satellite poller started",
		"sqlite_path", cfg.SQLiteDBPath,
		"kafka_brokers", cfg.KafkaBrokers,
		"kafka_topic", cfg.KafkaTopicHazardIngest,
		"max_lateness", cfg.MaxLateness,
	)

	var runErr error
	select {
	case <-rootCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		runErr = err
		logger.Error("component failed; shutting down", "error", err)
	}

	cancelWorkers()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("graceful health server shutdown failed", "error", err)
		_ = healthServer.Close()
	}

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
	case <-shutdownCtx.Done():
		logger.Warn("shutdown deadline reached; forcing health server close")
		_ = healthServer.Close()
		<-workersDone
	}

	logger.Info("satellite poller stopped")
	return runErr
}

// Load reads and validates application configuration
// from environment variables.
func Load() (*Config, error) {
	token, err := requiredEnv("SATELLITE_GROUP_TOKEN")
	if err != nil {
		return nil, err
	}

	reconnectDelay, err := durationWithUnit(
		"SATELLITE_RECONNECT_DELAY_SECONDS",
		DefaultReconnectDelay,
		time.Second,
	)
	if err != nil {
		return nil, err
	}

	maxLateness, err := durationWithUnit(
		"MAX_LATENESS_MINUTES",
		DefaultMaxLateness,
		time.Minute,
	)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		SatelliteBaseURL:       envOrDefault("SATELLITE_BASE_URL", DefaultSatelliteBaseURL),
		SatelliteGroupToken:    token,
		ReconnectDelay:         reconnectDelay,
		MaxLateness:            maxLateness,
		SQLiteDBPath:           envOrDefault("SQLITE_DB_PATH", DefaultSQLiteDBPath),
		KafkaBrokers:           csvEnvOrDefault("KAFKA_BROKERS", DefaultKafkaBrokers),
		KafkaTopicHazardIngest: envOrDefault("KAFKA_TOPIC_HAZARD_INGEST", DefaultKafkaTopicHazardIngest),
		HealthPort:             envOrDefault("HEALTH_PORT", DefaultHealthPort),
	}

	if err := validate(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// validate checks whether the configuration is valid.
func validate(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config must not be nil")
	}
	baseURL, err := url.Parse(strings.TrimSpace(cfg.SatelliteBaseURL))
	if err != nil || baseURL.Host == "" || (baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		return fmt.Errorf("SATELLITE_BASE_URL must be an absolute http or https URL")
	}
	if strings.TrimSpace(cfg.SatelliteGroupToken) == "" {
		return fmt.Errorf("SATELLITE_GROUP_TOKEN must be configured")
	}
	if cfg.ReconnectDelay <= 0 {
		return fmt.Errorf("SATELLITE_RECONNECT_DELAY_SECONDS must be positive")
	}
	if cfg.MaxLateness <= 0 {
		return fmt.Errorf("MAX_LATENESS_MINUTES must be positive")
	}
	if strings.TrimSpace(cfg.SQLiteDBPath) == "" {
		return fmt.Errorf("SQLITE_DB_PATH must be configured")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}
	for _, broker := range cfg.KafkaBrokers {
		if strings.TrimSpace(broker) == "" {
			return fmt.Errorf("KAFKA_BROKERS contains an empty broker address")
		}
	}
	if strings.TrimSpace(cfg.KafkaTopicHazardIngest) == "" {
		return fmt.Errorf("KAFKA_TOPIC_HAZARD_INGEST must be configured")
	}

	port, err := strconv.Atoi(cfg.HealthPort)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("HEALTH_PORT must be an integer between 1 and 65535")
	}

	return nil
}

// envOrDefault returns an environment variable
// or its fallback value.
func envOrDefault(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

// requiredEnv retrieves a mandatory environment variable.
func requiredEnv(key string) (string, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return "", fmt.Errorf("%s must be configured", key)
	}
	return value, nil
}

func durationWithUnit(
	key string,
	fallback time.Duration,
	unit time.Duration,
) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))

	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf(
			"%s must be a positive integer",
			key,
		)
	}

	return time.Duration(parsed) * unit, nil
}

// csvEnvOrDefault parses comma-separated environment values
// or returns the fallback values.
func csvEnvOrDefault(key, fallback string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		value = fallback
	}

	parts := strings.Split(value, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

// Options configures batching and retry polling.
type Options struct {
	BatchSize    int
	PollInterval time.Duration
	Logger       *slog.Logger
	Writer       KafkaWriter
	Encoder      Encoder
}

// Publisher continuously publishes pending SQLite outbox events to Kafka.
type Publisher struct {
	repo    OutboxRepository
	writer  KafkaWriter
	encoder Encoder
	topic   string
	batch   int
	poll    time.Duration
	logger  *slog.Logger
}

// NewKafkaPublisher creates a publisher. Pass opts.Writer only in tests or
// when the application supplies an already-configured Kafka writer.
func NewKafkaPublisher(
	brokers []string,
	topic string,
	repo OutboxRepository,
	opts Options,
) (*Publisher, error) {
	if repo == nil {
		return nil, errors.New("outbox repository is required")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("Kafka topic is required")
	}

	cleanBrokers := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		if value := strings.TrimSpace(broker); value != "" {
			cleanBrokers = append(cleanBrokers, value)
		}
	}
	if opts.Writer == nil && len(cleanBrokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}

	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBatchSize
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaultPollInterval
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Encoder == nil {
		opts.Encoder = HazardProtoJSONEncoder{}
	}

	writer := opts.Writer
	if writer == nil {
		writer = &kafka.Writer{
			Addr:         kafka.TCP(cleanBrokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			BatchSize:    opts.BatchSize,
			BatchTimeout: 10 * time.Millisecond,
			MaxAttempts:  5,
		}
	}

	return &Publisher{
		repo:    repo,
		writer:  writer,
		encoder: opts.Encoder,
		topic:   topic,
		batch:   opts.BatchSize,
		poll:    opts.PollInterval,
		logger:  opts.Logger.With("component", "satellite-kafka-publisher"),
	}, nil
}

// Run publishes pending events immediately, then polls for more until ctx ends.
// A failed batch remains pending in SQLite and is retried on the next cycle.
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.poll)
	defer ticker.Stop()

	for {
		if err := p.publishPending(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.logger.Error("failed to publish Kafka outbox batch", "error", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Close releases the Kafka writer. Call it during application shutdown.
func (p *Publisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

func (p *Publisher) publishPending(ctx context.Context) error {
	for {
		events, err := p.repo.GetPendingOutboxEvents(ctx, p.batch)
		if err != nil {
			return fmt.Errorf("load pending outbox events: %w", err)
		}
		if len(events) == 0 {
			return nil
		}

		messages := make([]kafka.Message, 0, len(events))

		for _, event := range events {
			payload, err := p.encoder.Encode(event)
			if err != nil {
				p.recordBatchFailure(ctx, events, err)
				return fmt.Errorf(
					"encode outbox event %q: %w",
					event.EventID,
					err,
				)
			}

			headers := []kafka.Header{
				{Key: "event_id", Value: []byte(event.EventID)},
				{Key: "source_ref_id", Value: []byte(event.SourceRefID)},
				{
					Key:   "revision",
					Value: []byte(strconv.FormatInt(event.Revision, 10)),
				},
			}

			// A tombstone has no JSON payload, so do not label it
			// as application/json.
			if payload != nil {
				headers = append(headers, kafka.Header{
					Key:   "content-type",
					Value: []byte("application/json"),
				})
			}

			messages = append(messages, kafka.Message{
				Key:     []byte(event.SourceRefID),
				Value:   payload,
				Time:    event.CreatedAt,
				Headers: headers,
			})
		}

		if err := p.writer.WriteMessages(ctx, messages...); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.recordBatchFailure(ctx, events, err)
			return fmt.Errorf("write %d messages to Kafka topic %q: %w", len(messages), p.topic, err)
		}

		publishedAt := time.Now().UTC()
		for _, event := range events {
			if err := p.repo.MarkOutboxPublished(ctx, event.EventID, publishedAt); err != nil {
				// Kafka may already contain the message. Leave this record pending;
				// retry is safe only when the consumer deduplicates event_id/revision.
				return fmt.Errorf("mark outbox event %q as published: %w", event.EventID, err)
			}
		}

		p.logger.Debug("published Kafka outbox batch",
			"topic", p.topic,
			"event_count", len(events),
			"first_event_id", events[0].EventID,
			"last_event_id", events[len(events)-1].EventID,
		)

		if len(events) < p.batch {
			return nil
		}
	}
}

func (p *Publisher) recordBatchFailure(ctx context.Context, events []domain.OutboxEvent, cause error) {
	for _, event := range events {
		if err := p.repo.MarkOutboxFailed(ctx, event.EventID, cause.Error()); err != nil {
			p.logger.Error("could not record outbox publish failure",
				"event_id", event.EventID,
				"error", err,
			)
		}
	}
}

type HazardProtoJSONEncoder struct{}

func (HazardProtoJSONEncoder) Encode(
	event domain.OutboxEvent,
) ([]byte, error) {

	var envelope processing.OutboxEnvelope

	if err := json.Unmarshal(event.Payload, &envelope); err != nil {
		return nil, fmt.Errorf(
			"decode outbox envelope: %w", err,
		)
	}

	if envelope.SourceRefID == "" {
		return nil, fmt.Errorf("outbox envelope has no source_ref_id")
	}

	if envelope.SourceRefID != event.SourceRefID {
		return nil, fmt.Errorf("outbox source_ref_id mismatch")
	}

	if envelope.Revision != event.Revision {
		return nil, fmt.Errorf("outbox revision mismatch")
	}

	switch envelope.Operation {

	case "upsert":
		if envelope.Hazard == nil {
			return nil, fmt.Errorf("upsert envelope has no hazard")
		}

		if envelope.Hazard.SourceRefID != envelope.SourceRefID {
			return nil, fmt.Errorf("hazard source_ref_id mismatch")
		}

		hazard, err := toProtoHazard(*envelope.Hazard)
		if err != nil {
			return nil, err
		}

		payload, err := protojson.MarshalOptions{
			UseProtoNames: true,
		}.Marshal(hazard)

		if err != nil {
			return nil, fmt.Errorf(
				"marshal Protobuf JSON: %w", err,
			)
		}

		return payload, nil

	case "retract":
		if envelope.Hazard != nil {
			return nil, fmt.Errorf(
				"retract envelope must not contain a hazard",
			)
		}

		// nil value means Kafka tombstone.
		return nil, nil

	default:
		return nil, fmt.Errorf(
			"unsupported Kafka operation: %s",
			envelope.Operation,
		)
	}
}

func newHealthServer(port string, db interface{ PingContext(context.Context) error }) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})

	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready\n")
	})

	return &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
}
