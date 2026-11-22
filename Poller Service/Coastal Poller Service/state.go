package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	stateDocumentID     = "sensor-pantai-adapter"
	stateCollection     = "coastal_adapter_state"
	retryableCollection = "coastal_adapter_retryable"
	initialRetryDelay   = 1 * time.Second
)

// SourceStatus stores last-known-good metadata for the Sensor Pantai source.
// It intentionally stores metadata rather than duplicating HazardEvent data.
type SourceStatus struct {
	UpstreamStatus     string     `bson:"upstream_status,omitempty" json:"upstream_status,omitempty"`
	LastSuccessAt      *time.Time `bson:"last_success_at,omitempty" json:"last_success_at,omitempty"`
	LastDataAt         *time.Time `bson:"last_data_at,omitempty" json:"last_data_at,omitempty"`
	LastDataReceivedAt *time.Time `bson:"last_data_received_at,omitempty" json:"last_data_received_at,omitempty"`
	LastObservationID  string     `bson:"last_observation_id,omitempty" json:"last_observation_id,omitempty"`
	DataStatus         string     `bson:"data_status,omitempty" json:"data_status,omitempty"`
	LastFailureAt      *time.Time `bson:"last_failure_at,omitempty" json:"last_failure_at,omitempty"`
	LastError          string     `bson:"last_error,omitempty" json:"last_error,omitempty"`
	UpdatedAt          time.Time  `bson:"updated_at" json:"updated_at"`
}

type adapterStateDocument struct {
	ID                  string     `bson:"_id"`
	Cursor              string     `bson:"cursor"`
	CheckpointUpdatedAt *time.Time `bson:"checkpoint_updated_at,omitempty"`
	SourceStatus        `bson:",inline"`
}

// RetryableObservation is a durable retry-queue record. Unknown station
// observations are stored here until refreshed station metadata allows them
// to be normalized. Raw contains the exact original observation bytes.
type RetryableObservation struct {
	MessageID     string     `bson:"_id" json:"message_id"`
	ObservationID string     `bson:"observation_id,omitempty" json:"observation_id,omitempty"`
	CorrelationID string     `bson:"correlation_id,omitempty" json:"correlation_id,omitempty"`
	Reason        string     `bson:"reason" json:"reason"`
	Raw           []byte     `bson:"raw" json:"raw"`
	ReceivedAt    time.Time  `bson:"received_at" json:"received_at"`
	ReferenceAt   *time.Time `bson:"reference_at,omitempty" json:"reference_at,omitempty"`
	NextAttemptAt time.Time  `bson:"next_attempt_at" json:"next_attempt_at"`
	LastAttemptAt *time.Time `bson:"last_attempt_at,omitempty" json:"last_attempt_at,omitempty"`
	Attempts      int        `bson:"attempts" json:"attempts"`
	LastError     string     `bson:"last_error,omitempty" json:"last_error,omitempty"`
}

// StateStore persists the opaque feed cursor, source health metadata, and
// retryable observations in an adapter-owned MongoDB database. It must not
// point to Aggregate's HazardEvent database/collections. Configure the URI
// with replication and credentials restricted to this adapter's state DB.
type StateStore struct {
	client     *mongo.Client
	collection *mongo.Collection
	retryable  *mongo.Collection
}

// NewStateStore connects to the database specified in PANTAI_STATE_URI.
// Writes use majority acknowledgment so a successful checkpoint/retry enqueue
// is not merely acknowledged by one transient primary.
func NewStateStore(ctx context.Context, stateURI string) (*StateStore, error) {
	if strings.TrimSpace(stateURI) == "" {
		return nil, fmt.Errorf("PANTAI_STATE_URI must not be empty")
	}

	databaseName, err := databaseNameFromMongoURI(stateURI)
	if err != nil {
		return nil, err
	}

	clientOptions := options.Client().
		ApplyURI(stateURI).
		SetServerSelectionTimeout(5 * time.Second).
		SetConnectTimeout(5 * time.Second).
		SetWriteConcern(writeconcern.Majority()).
		SetReadConcern(readconcern.Majority())

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, fmt.Errorf("connect to adapter state store: %w", redactMongoURIDetails(err))
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("ping adapter state store: %w", redactMongoURIDetails(err))
	}

	database := client.Database(databaseName)
	store := &StateStore{
		client:     client,
		collection: database.Collection(stateCollection),
		retryable:  database.Collection(retryableCollection),
	}
	_, err = store.retryable.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "next_attempt_at", Value: 1}},
		Options: options.Index().SetName("retry_due_at"),
	})
	if err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("create retry queue index: %w", redactMongoURIDetails(err))
	}
	return store, nil
}

// LoadCheckpoint returns the last cursor whose complete page has been
// durably published to Kafka or durably retained in the retry queue.
func (s *StateStore) LoadCheckpoint(ctx context.Context) (string, error) {
	if err := s.ensureReady(); err != nil {
		return "", err
	}
	var document struct {
		Cursor string `bson:"cursor"`
	}
	err := s.collection.FindOne(ctx, bson.D{{Key: "_id", Value: stateDocumentID}}).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load Sensor Pantai checkpoint: %w", redactMongoURIDetails(err))
	}
	return document.Cursor, nil
}

// SaveCheckpoint persists the next cursor. Prefer CommitPage so cursor and
// source freshness metadata are updated atomically.
func (s *StateStore) SaveCheckpoint(ctx context.Context, cursor string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	now := time.Now().UTC()
	update := bson.D{{Key: "$set", Value: bson.D{
		{Key: "cursor", Value: cursor},
		{Key: "checkpoint_updated_at", Value: now},
		{Key: "updated_at", Value: now},
	}}}
	if _, err := s.collection.UpdateOne(ctx, bson.D{{Key: "_id", Value: stateDocumentID}}, update, options.Update().SetUpsert(true)); err != nil {
		return fmt.Errorf("save Sensor Pantai checkpoint: %w", redactMongoURIDetails(err))
	}
	return nil
}

// LoadSourceStatus returns the latest persisted source freshness/status data.
func (s *StateStore) LoadSourceStatus(ctx context.Context) (SourceStatus, error) {
	if err := s.ensureReady(); err != nil {
		return SourceStatus{}, err
	}
	var document adapterStateDocument
	err := s.collection.FindOne(ctx, bson.D{{Key: "_id", Value: stateDocumentID}}).Decode(&document)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return SourceStatus{}, nil
	}
	if err != nil {
		return SourceStatus{}, fmt.Errorf("load Sensor Pantai source status: %w", redactMongoURIDetails(err))
	}
	return document.SourceStatus, nil
}

// RecordSuccessfulFetch records a successful observations request. Data status
// is recomputed from the age of last-known-good data, even when this page has no
// valid observation. maxDataAge must be the same explicit policy used by validation.
func (s *StateStore) RecordSuccessfulFetch(
	ctx context.Context, fetchedAt time.Time, lastDataAt *time.Time,
	observationID string, sensorNow *time.Time, maxDataAge time.Duration,
) error {
	return s.updateSourceState(ctx, lastDataAt, observationID, fetchedAt, sensorNow, maxDataAge, bson.D{
		{Key: "last_success_at", Value: fetchedAt.UTC()},
		{Key: "upstream_status", Value: "available"},
	}, true)
}

// RecordSourceFailure marks upstream connectivity unavailable while keeping
// data freshness derived from last-known-good observation age.
func (s *StateStore) RecordSourceFailure(ctx context.Context, failure error, failedAt time.Time, maxDataAge time.Duration) error {
	message := "unknown upstream failure"
	if failure != nil {
		message = failure.Error()
	}
	return s.updateSourceState(ctx, nil, "", failedAt, nil, maxDataAge, bson.D{
		{Key: "upstream_status", Value: "unavailable"},
		{Key: "last_failure_at", Value: failedAt.UTC()},
		{Key: "last_error", Value: truncateString(message, 1024)},
	}, false)
}

// RefreshDataStatus recomputes fresh/stale/unavailable on startup and during
// polling. It prevents a persisted "fresh" value from remaining fresh forever.
func (s *StateStore) RefreshDataStatus(ctx context.Context, now time.Time, maxDataAge time.Duration) error {
	return s.updateSourceState(ctx, nil, "", now, nil, maxDataAge, nil, false)
}

// MarkDataUnavailable records that no usable last-known-good observation exists.
func (s *StateStore) MarkDataUnavailable(ctx context.Context, at time.Time, reason string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if at.IsZero() {
		return fmt.Errorf("at must not be zero")
	}
	_, err := s.collection.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: stateDocumentID}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "data_status", Value: "unavailable"},
			{Key: "last_error", Value: truncateString(reason, 1024)},
			{Key: "updated_at", Value: at.UTC()},
		}}}, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("mark Sensor Pantai data unavailable: %w", redactMongoURIDetails(err))
	}
	return nil
}

// RecordLastGoodObservation advances last-known-good metadata after a
// retryable observation has been recovered and its HazardEvent is durably
// acknowledged by Kafka. It never moves the stored observation time backward.
func (s *StateStore) RecordLastGoodObservation(
	ctx context.Context, observedAt time.Time, observationID string,
	recordedAt time.Time, maxDataAge time.Duration,
) error {
	if observedAt.IsZero() {
		return fmt.Errorf("observedAt must not be zero")
	}
	return s.updateSourceState(ctx, &observedAt, observationID, recordedAt, nil, maxDataAge, nil, false)
}

// CommitPage atomically records the cursor and freshness metadata after every
// item in the page has been durably published or retained for retry/quarantine.
func (s *StateStore) CommitPage(
	ctx context.Context, cursor string, fetchedAt time.Time, lastDataAt *time.Time,
	observationID string, sensorNow *time.Time, maxDataAge time.Duration,
) error {
	return s.updateSourceState(ctx, lastDataAt, observationID, fetchedAt, sensorNow, maxDataAge, bson.D{
		{Key: "cursor", Value: cursor},
		{Key: "checkpoint_updated_at", Value: fetchedAt.UTC()},
		{Key: "last_success_at", Value: fetchedAt.UTC()},
		{Key: "upstream_status", Value: "available"},
	}, true)
}

// updateSourceState evaluates last-good time and freshness inside one MongoDB
// update. Polling and recovery can publish concurrently without an older retry
// overwriting newer data metadata. Only the polling caller supplies a cursor.
func (s *StateStore) updateSourceState(
	ctx context.Context, candidate *time.Time, observationID string, now time.Time,
	sensorNow *time.Time, maxAge time.Duration, fields bson.D, clearError bool,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if now.IsZero() || maxAge <= 0 {
		return fmt.Errorf("time and max data age must be valid")
	}
	literal := func(value any) bson.D { return bson.D{{Key: "$literal", Value: value}} }
	set := bson.D{{Key: "updated_at", Value: literal(now.UTC())}}
	for _, field := range fields {
		set = append(set, bson.E{Key: field.Key, Value: literal(field.Value)})
	}
	if candidate != nil && !candidate.IsZero() {
		at := literal(candidate.UTC())
		received := literal(now.UTC())
		previous := bson.D{{Key: "$ifNull", Value: bson.A{"$last_data_at", nil}}}
		previousReceived := bson.D{{Key: "$ifNull", Value: bson.A{"$last_data_received_at", nil}}}
		if strings.TrimSpace(observationID) != "" {
			set = append(set, bson.E{Key: "last_observation_id", Value: bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$gte", Value: bson.A{at, previous}}},
				literal(strings.TrimSpace(observationID)),
				"$last_observation_id",
			}}}})
		}
		set = append(set, bson.E{Key: "last_data_at", Value: bson.D{{Key: "$max", Value: bson.A{previous, at}}}})
		set = append(set, bson.E{Key: "last_data_received_at", Value: bson.D{{Key: "$max", Value: bson.A{previousReceived, received}}}})
	}
	receivedCutoff := bson.D{{Key: "$gte", Value: bson.A{
		bson.D{{Key: "$ifNull", Value: bson.A{"$last_data_received_at", "$last_data_at"}}},
		literal(now.UTC().Add(-maxAge)),
	}}}
	freshExpr := any(receivedCutoff)
	if sensorNow != nil && !sensorNow.IsZero() {
		sensorCutoff := bson.D{{Key: "$gte", Value: bson.A{
			"$last_data_at",
			literal(sensorNow.UTC().Add(-maxAge)),
		}}}
		freshExpr = bson.D{{Key: "$and", Value: bson.A{receivedCutoff, sensorCutoff}}}
	}
	status := bson.D{{Key: "$cond", Value: bson.A{
		bson.D{{Key: "$eq", Value: bson.A{
			bson.D{{Key: "$ifNull", Value: bson.A{"$last_data_at", nil}}}, nil,
		}}},
		"unavailable",
		bson.D{{Key: "$cond", Value: bson.A{
			freshExpr,
			"fresh", "stale",
		}}},
	}}}
	pipeline := mongo.Pipeline{
		bson.D{{Key: "$set", Value: set}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "data_status", Value: status}}}},
	}
	if clearError {
		pipeline = append(pipeline, bson.D{{Key: "$unset", Value: "last_error"}})
	}
	_, err := s.collection.UpdateOne(ctx, bson.D{{Key: "_id", Value: stateDocumentID}},
		pipeline, options.Update().SetUpsert(true))
	if err != nil {
		return fmt.Errorf("update Sensor Pantai source state: %w", redactMongoURIDetails(err))
	}
	return nil
}

// SaveRetryableObservation durably enqueues an unknown-station observation.
// The deterministic ID makes replay of a page idempotent.
func (s *StateStore) SaveRetryableObservation(
	ctx context.Context,
	record ObservationRecord,
	receivedAt time.Time,
	referenceAt time.Time,
	correlationID string,
) (RetryableObservation, error) {
	if err := s.ensureReady(); err != nil {
		return RetryableObservation{}, err
	}
	if len(record.Raw) == 0 || record.Observation == nil {
		return RetryableObservation{}, fmt.Errorf("retryable observation requires raw bytes and a decoded observation")
	}
	if receivedAt.IsZero() {
		return RetryableObservation{}, fmt.Errorf("receivedAt must not be zero")
	}
	var refPtr *time.Time
	if !referenceAt.IsZero() {
		ref := referenceAt.UTC()
		refPtr = &ref
	}
	id := deterministicQuarantineID(coastalKafkaSource, string(ReasonUnknownStation), record.Raw)
	document := RetryableObservation{
		MessageID:     id,
		ObservationID: strings.TrimSpace(record.Observation.ObservationID),
		CorrelationID: correlationID,
		Reason:        string(ReasonUnknownStation),
		Raw:           append([]byte(nil), record.Raw...),
		ReceivedAt:    receivedAt.UTC(),
		ReferenceAt:   refPtr,
		NextAttemptAt: receivedAt.UTC().Add(initialRetryDelay),
		Attempts:      0,
	}
	_, err := s.retryable.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$setOnInsert", Value: document}},
		options.Update().SetUpsert(true))
	if err != nil {
		return RetryableObservation{}, fmt.Errorf("enqueue retryable Sensor Pantai observation: %w", redactMongoURIDetails(err))
	}
	return document, nil
}

// ListRetryableObservations returns due records in deterministic retry order.
func (s *StateStore) ListRetryableObservations(ctx context.Context, now time.Time, limit int) ([]RetryableObservation, error) {
	if err := s.ensureReady(); err != nil {
		return nil, err
	}
	if now.IsZero() || limit <= 0 {
		return nil, fmt.Errorf("now and retry limit must be valid")
	}
	cursor, err := s.retryable.Find(ctx,
		bson.D{{Key: "next_attempt_at", Value: bson.D{{Key: "$lte", Value: now.UTC()}}}},
		options.Find().SetSort(bson.D{{Key: "next_attempt_at", Value: 1}, {Key: "received_at", Value: 1}}).SetLimit(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("list retryable observations: %w", redactMongoURIDetails(err))
	}
	defer cursor.Close(ctx)

	var items []RetryableObservation
	if err := cursor.All(ctx, &items); err != nil {
		return nil, fmt.Errorf("decode retryable observations: %w", redactMongoURIDetails(err))
	}
	return items, nil
}

// MarkRetryAttempt postpones the next retry with a caller-selected backoff.
func (s *StateStore) MarkRetryAttempt(ctx context.Context, messageID string, attemptedAt, nextAttemptAt time.Time, reason string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if strings.TrimSpace(messageID) == "" || attemptedAt.IsZero() || nextAttemptAt.IsZero() {
		return fmt.Errorf("messageID, attemptedAt, and nextAttemptAt are required")
	}
	_, err := s.retryable.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: messageID}},
		bson.D{
			{Key: "$inc", Value: bson.D{{Key: "attempts", Value: 1}}},
			{Key: "$set", Value: bson.D{
				{Key: "last_attempt_at", Value: attemptedAt.UTC()},
				{Key: "next_attempt_at", Value: nextAttemptAt.UTC()},
				{Key: "last_error", Value: truncateString(reason, 512)},
			}},
		})
	if err != nil {
		return fmt.Errorf("update retryable observation schedule: %w", redactMongoURIDetails(err))
	}
	return nil
}

// DeleteRetryableObservation removes an item only after it has been published
// successfully or moved to permanent quarantine with a durable Kafka ACK.
func (s *StateStore) DeleteRetryableObservation(ctx context.Context, messageID string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if strings.TrimSpace(messageID) == "" {
		return fmt.Errorf("messageID must not be empty")
	}
	if _, err := s.retryable.DeleteOne(ctx, bson.D{{Key: "_id", Value: messageID}}); err != nil {
		return fmt.Errorf("delete recovered retryable observation: %w", redactMongoURIDetails(err))
	}
	return nil
}

func (s *StateStore) latestDataAt(ctx context.Context, candidate *time.Time) (*time.Time, error) {
	var existing struct {
		LastDataAt *time.Time `bson:"last_data_at"`
	}
	err := s.collection.FindOne(ctx, bson.D{{Key: "_id", Value: stateDocumentID}}).Decode(&existing)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("read last-known-good observation time: %w", redactMongoURIDetails(err))
	}
	latest := existing.LastDataAt
	if candidate != nil && !candidate.IsZero() && (latest == nil || candidate.After(*latest)) {
		value := candidate.UTC()
		latest = &value
	}
	return latest, nil
}

func freshnessStatus(lastDataAt *time.Time, now time.Time, maxAge time.Duration) string {
	if lastDataAt == nil || lastDataAt.IsZero() {
		return "unavailable"
	}
	if now.Sub(*lastDataAt) <= maxAge {
		return "fresh"
	}
	return "stale"
}

// Close disconnects the MongoDB client. Call only after all workers using it stop.
func (s *StateStore) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}
	if err := s.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("disconnect adapter state store: %w", redactMongoURIDetails(err))
	}
	return nil
}

func (s *StateStore) ensureReady() error {
	if s == nil || s.client == nil || s.collection == nil || s.retryable == nil {
		return fmt.Errorf("adapter state store is not initialized")
	}
	return nil
}

func databaseNameFromMongoURI(rawURI string) (string, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return "", fmt.Errorf("PANTAI_STATE_URI is not a valid MongoDB URI")
	}
	if parsed.Scheme != "mongodb" && parsed.Scheme != "mongodb+srv" {
		return "", fmt.Errorf("PANTAI_STATE_URI must use mongodb:// or mongodb+srv://")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("PANTAI_STATE_URI must include a host")
	}
	databaseName := strings.Trim(strings.TrimSpace(parsed.EscapedPath()), "/")
	if databaseName == "" || strings.Contains(databaseName, "/") {
		return "", fmt.Errorf("PANTAI_STATE_URI must include exactly one database name in its path")
	}
	decodedName, err := url.PathUnescape(databaseName)
	if err != nil || strings.TrimSpace(decodedName) == "" {
		return "", fmt.Errorf("PANTAI_STATE_URI contains an invalid database name")
	}
	return decodedName, nil
}

func truncateString(value string, maxBytes int) string {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	return value[:maxBytes]
}

// redactMongoURIDetails avoids returning a raw driver error that may contain
// a credential-bearing URI.
func redactMongoURIDetails(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("MongoDB driver operation failed (%T)", err)
}
