package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	coastalKafkaSource = "SENSOR_PANTAI"
	kafkaContentType   = "application/json"
)

// QuarantineMessage preserves the exact original item bytes. []byte is encoded
// as base64 in JSON, so malformed UTF-8 or malformed JSON is not lost while
// wrapping the quarantine metadata in a valid Kafka JSON message.
type QuarantineMessage struct {
	MessageID     string    `json:"message_id"`
	Source        string    `json:"source"`
	ReceivedAt    time.Time `json:"received_at"`
	Reason        string    `json:"reason"`
	Retryable     bool      `json:"retryable"`
	Status        string    `json:"status"`
	Attempts      int       `json:"attempts"`
	ObservationID string    `json:"observation_id,omitempty"`
	CorrelationID string    `json:"correlation_id"`
	PayloadBase64 []byte    `json:"payload_base64"`
}

// messageWriter is implemented by kafka.Writer and keeps the publisher easy to
// unit test without a running broker.
type messageWriter interface {
	WriteMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

// KafkaPublisher synchronously publishes to the two durable ingestion topics.
// WriteMessages returns only after the broker acknowledgment required by the
// writer configuration has been received.
type KafkaPublisher struct {
	ingestTopic      string
	quarantineTopic  string
	ingestWriter     messageWriter
	quarantineWriter messageWriter
}

// NewKafkaPublisher creates synchronous, hash-partitioned writers. RequireAll
// is important: the topic must also be configured with an appropriate
// replication factor and min.insync.replicas for node-loss durability.
func NewKafkaPublisher(cfg Config) (*KafkaPublisher, error) {
	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("at least one Kafka broker is required")
	}
	if strings.TrimSpace(cfg.KafkaTopicPantaiIngest) == "" {
		return nil, fmt.Errorf("Kafka ingest topic must not be empty")
	}
	if strings.TrimSpace(cfg.KafkaTopicPantaiQuarantine) == "" {
		return nil, fmt.Errorf("Kafka quarantine topic must not be empty")
	}
	if cfg.KafkaTopicPantaiIngest == cfg.KafkaTopicPantaiQuarantine {
		return nil, fmt.Errorf("Kafka ingest and quarantine topics must be different")
	}

	address := kafka.TCP(cfg.KafkaBrokers...)
	ingestWriter := newDurableKafkaWriter(address, cfg.KafkaTopicPantaiIngest)
	quarantineWriter := newDurableKafkaWriter(address, cfg.KafkaTopicPantaiQuarantine)

	return &KafkaPublisher{
		ingestTopic:      cfg.KafkaTopicPantaiIngest,
		quarantineTopic:  cfg.KafkaTopicPantaiQuarantine,
		ingestWriter:     ingestWriter,
		quarantineWriter: quarantineWriter,
	}, nil
}

func newDurableKafkaWriter(address net.Addr, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   address,
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		BatchTimeout:           10 * time.Millisecond,
		ReadTimeout:            10 * time.Second,
		WriteTimeout:           10 * time.Second,
		MaxAttempts:            5,
		AllowAutoTopicCreation: false,
	}
}

// PublishHazard publishes one normalized COASTAL event. The deterministic
// hazard_id is used as the Kafka key, matching the convention used by the
// existing BMKG/PVMBG pollers and keeping replay of the same event in one
// partition.
func (p *KafkaPublisher) PublishHazard(
	ctx context.Context,
	event HazardEvent,
	correlationID string,
) error {
	if err := p.ensureReady(); err != nil {
		return err
	}
	if strings.TrimSpace(event.HazardID) == "" || strings.TrimSpace(event.SourceRefID) == "" {
		return fmt.Errorf("hazard_id and source_ref_id are required for Kafka publication")
	}
	if event.Source != coastalSource || event.HazardType != coastalHazardType {
		return fmt.Errorf("refusing to publish non-coastal event: source=%q hazard_type=%q", event.Source, event.HazardType)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal coastal HazardEvent: %w", err)
	}

	message := kafka.Message{
		Key:     []byte(event.HazardID),
		Value:   payload,
		Time:    event.IngestedAt.UTC(),
		Headers: kafkaHeaders(correlationID, event.HazardID),
	}
	if message.Time.IsZero() {
		message.Time = time.Now().UTC()
	}
	if err := p.ingestWriter.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish HazardEvent to topic %q: %w", p.ingestTopic, err)
	}
	return nil
}

// PublishQuarantine publishes an invalid observation without converting it
// into a HazardEvent. The original bytes are base64-encoded, preserving even
// malformed JSON/UTF-8 payloads exactly. The deterministic message ID makes a
// replay of the same payload/reason idempotently identifiable downstream.
func (p *KafkaPublisher) PublishQuarantine(
	ctx context.Context,
	record ObservationRecord,
	reason ValidationReason,
	receivedAt time.Time,
	correlationID string,
) error {
	if err := p.ensureReady(); err != nil {
		return err
	}
	if len(record.Raw) == 0 {
		return fmt.Errorf("cannot quarantine an observation with an empty raw payload")
	}
	if strings.TrimSpace(string(reason)) == "" {
		return fmt.Errorf("quarantine reason must not be empty")
	}
	if receivedAt.IsZero() {
		return fmt.Errorf("quarantine receivedAt must not be zero")
	}

	raw := append([]byte(nil), record.Raw...)
	messageID := deterministicQuarantineID(coastalKafkaSource, string(reason), raw)
	retryable := reason == ReasonUnknownStation
	status := "parked"
	if retryable {
		status = "retrying"
	}

	observationID := ""
	if record.Observation != nil {
		observationID = strings.TrimSpace(record.Observation.ObservationID)
	}
	if strings.TrimSpace(correlationID) == "" {
		correlationID = messageID
	}

	quarantine := QuarantineMessage{
		MessageID:     messageID,
		Source:        coastalKafkaSource,
		ReceivedAt:    receivedAt.UTC(),
		Reason:        string(reason),
		Retryable:     retryable,
		Status:        status,
		Attempts:      1,
		ObservationID: observationID,
		CorrelationID: correlationID,
		PayloadBase64: raw,
	}

	payload, err := json.Marshal(quarantine)
	if err != nil {
		return fmt.Errorf("marshal quarantine message: %w", err)
	}

	message := kafka.Message{
		Key:     []byte(messageID),
		Value:   payload,
		Time:    receivedAt.UTC(),
		Headers: kafkaHeaders(correlationID, messageID),
	}
	if err := p.quarantineWriter.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish quarantine message to topic %q: %w", p.quarantineTopic, err)
	}
	return nil
}

// PublishRawQuarantine is useful when an observation could not be decoded into
// ObservationRecord at all. It keeps the raw bytes and uses the same envelope
// and durability rules as PublishQuarantine.
func (p *KafkaPublisher) PublishRawQuarantine(
	ctx context.Context,
	raw []byte,
	reason ValidationReason,
	receivedAt time.Time,
	correlationID string,
) error {
	record := ObservationRecord{Raw: append([]byte(nil), raw...)}
	return p.PublishQuarantine(ctx, record, reason, receivedAt, correlationID)
}

// Close flushes and closes both synchronous writers. Call during graceful
// shutdown after the polling loop has stopped.
func (p *KafkaPublisher) Close() error {
	if p == nil {
		return nil
	}

	var firstErr error
	if p.ingestWriter != nil {
		if err := p.ingestWriter.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close Kafka ingest writer: %w", err)
		}
	}
	if p.quarantineWriter != nil {
		if err := p.quarantineWriter.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close Kafka quarantine writer: %w", err)
		}
	}
	return firstErr
}

func (p *KafkaPublisher) ensureReady() error {
	if p == nil || p.ingestWriter == nil || p.quarantineWriter == nil {
		return fmt.Errorf("Kafka publisher is not initialized")
	}
	return nil
}

func kafkaHeaders(correlationID, messageID string) []kafka.Header {
	headers := []kafka.Header{
		{Key: "content-type", Value: []byte(kafkaContentType)},
	}
	if strings.TrimSpace(correlationID) != "" {
		headers = append(headers, kafka.Header{Key: "correlation-id", Value: []byte(correlationID)})
	}
	if strings.TrimSpace(messageID) != "" {
		headers = append(headers, kafka.Header{Key: "message-id", Value: []byte(messageID)})
	}
	return headers
}

func deterministicQuarantineID(source, reason string, payload []byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(source))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(reason))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(payload)
	return "q-" + hex.EncodeToString(hash.Sum(nil))
}

// DecodeQuarantinePayload returns the exact original bytes from the base64
// field. This helper is useful for inspection tools and tests.
func DecodeQuarantinePayload(message QuarantineMessage) ([]byte, error) {
	if len(message.PayloadBase64) == 0 {
		return nil, fmt.Errorf("quarantine message has no payload")
	}
	// JSON unmarshalling into []byte already base64-decodes the field. This
	// helper exists for callers that have the encoded representation as text.
	return append([]byte(nil), message.PayloadBase64...), nil
}

// DecodeQuarantinePayloadBase64 decodes a separately captured base64 string.
func DecodeQuarantinePayloadBase64(encoded string) ([]byte, error) {
	payload, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode quarantine payload: %w", err)
	}
	return payload, nil
}
