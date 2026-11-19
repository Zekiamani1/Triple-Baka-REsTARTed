package main

import (
	"context"
	"fmt"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"net/url"
	"os"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"satellitepoller/domain"
)

const (
	DefaultSatelliteBaseURL       = "https://satellite.naufarrel.dev"
	DefaultReconnectDelay         = 5 * time.Second
	DefaultMaxLateness            = 30 * time.Minute
	DefaultSQLiteDBPath           = "/var/lib/satellite-poller/state.db"
	DefaultKafkaBrokers           = "kafka-aggregate:29092"
	DefaultKafkaTopicHazardIngest = "hazard-ingest"
	DefaultHealthPort             = "8085"
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


const (
	defaultBatchSize    = 100
	defaultPollInterval = time.Second
)

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

// OutboxJSONEncoder publishes the JSON OutboxEnvelope created by processing.
// It preserves operation, source_ref_id, revision, hazard, and reason fields.
type OutboxJSONEncoder struct{}

// Load reads and validates application configuration
// from environment variables.
func Load() (*Config, error) {
	token, err := requiredEnv("SATELLITE_GROUP_TOKEN")
	if err != nil {
		return nil, err
	}

	reconnectDelay, err := durationOrDefault(
		"SATELLITE_RECONNECT_DELAY_SECONDS",
		DefaultReconnectDelay,
	)
	if err != nil {
		return nil, err
	}

	maxLateness, err := durationOrDefault(
		"MAX_LATENESS_MINUTES",
		DefaultMaxLateness,
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

// durationOrDefault parses a duration from environment variables
// or returns the fallback value.
func durationOrDefault(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}

	return time.Duration(parsed) * time.Second, nil
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

func (OutboxJSONEncoder) Encode(event domain.OutboxEvent) ([]byte, error) {
	if len(event.Payload) == 0 || !json.Valid(event.Payload) {
		return nil, fmt.Errorf("outbox event %q contains invalid JSON", event.EventID)
	}
	return append([]byte(nil), event.Payload...), nil
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
		opts.Encoder = OutboxJSONEncoder{}
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
				return fmt.Errorf("encode outbox event %q: %w", event.EventID, err)
			}

			messages = append(messages, kafka.Message{
				Key:   []byte(event.SourceRefID),
				Value: payload,
				Time:  event.CreatedAt,
				Headers: []kafka.Header{
					{Key: "content-type", Value: []byte("application/json")},
					{Key: "event_id", Value: []byte(event.EventID)},
					{Key: "source_ref_id", Value: []byte(event.SourceRefID)},
					{Key: "revision", Value: []byte(strconv.FormatInt(event.Revision, 10))},
				},
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
