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
	stateDocumentID = "sensor-pantai-adapter"
	stateCollection = "coastal_adapter_state"
)

// SourceStatus stores last-known-good metadata for the Sensor Pantai source.
// It intentionally stores metadata rather than duplicating HazardEvent data.
type SourceStatus struct {
	UpstreamStatus    string     `bson:"upstream_status,omitempty" json:"upstream_status,omitempty"`
	LastSuccessAt     *time.Time `bson:"last_success_at,omitempty" json:"last_success_at,omitempty"`
	LastDataAt        *time.Time `bson:"last_data_at,omitempty" json:"last_data_at,omitempty"`
	LastObservationID string     `bson:"last_observation_id,omitempty" json:"last_observation_id,omitempty"`
	DataStatus        string     `bson:"data_status,omitempty" json:"data_status,omitempty"`
	LastFailureAt     *time.Time `bson:"last_failure_at,omitempty" json:"last_failure_at,omitempty"`
	LastError         string     `bson:"last_error,omitempty" json:"last_error,omitempty"`
	UpdatedAt         time.Time  `bson:"updated_at" json:"updated_at"`
}

type adapterStateDocument struct {
	ID                  string     `bson:"_id"`
	Cursor              string     `bson:"cursor"`
	CheckpointUpdatedAt *time.Time `bson:"checkpoint_updated_at,omitempty"`
	SourceStatus        `bson:",inline"`
}

// StateStore persists the opaque feed cursor and source health metadata in an
// adapter-owned MongoDB database. It must not point to Aggregate's HazardEvent
// database/collections. Configure the MongoDB URI with a replica set and
// credentials restricted to this adapter's state database.
type StateStore struct {
	client     *mongo.Client
	collection *mongo.Collection
}

// NewStateStore connects to the database specified in PANTAI_STATE_URI.
// The URI must include a database path, for example:
// mongodb://user:password@mongo-host:27017/pantai_adapter?replicaSet=rs0
// Writes use majority acknowledgment so a successful checkpoint is not merely
// acknowledged by one transient primary.
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

	return &StateStore{
		client:     client,
		collection: client.Database(databaseName).Collection(stateCollection),
	}, nil
}

// LoadCheckpoint returns the last cursor whose complete page was durably
// published to Kafka. An empty cursor means that the adapter has no checkpoint
// yet and must follow the source's documented initial-cursor behavior.
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
		return "", fmt.Errorf("load Sensor Pantai checkpoint: %w", err)
	}
	return document.Cursor, nil
}

// SaveCheckpoint persists the next cursor only after every observation in the
// fetched page has been durably acknowledged by its Kafka destination.
// Checkpoints are opaque strings and must never be numerically compared.
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
	if _, err := s.collection.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: stateDocumentID}},
		update,
		options.Update().SetUpsert(true),
	); err != nil {
		return fmt.Errorf("save Sensor Pantai checkpoint: %w", err)
	}
	return nil
}

// LoadSourceStatus returns the latest persisted source freshness/status data.
// A zero-value result means that no successful fetch has been recorded yet.
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
		return SourceStatus{}, fmt.Errorf("load Sensor Pantai source status: %w", err)
	}
	return document.SourceStatus, nil
}

// RecordSuccessfulFetch records that an authenticated observations request
// succeeded. lastDataAt and observationID may be empty for a valid empty page;
// in that case the last-known-good data metadata is preserved.
func (s *StateStore) RecordSuccessfulFetch(
	ctx context.Context,
	fetchedAt time.Time,
	lastDataAt *time.Time,
	observationID string,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if fetchedAt.IsZero() {
		return fmt.Errorf("fetchedAt must not be zero")
	}

	fetchedAt = fetchedAt.UTC()
	setFields := bson.D{
		{Key: "last_success_at", Value: fetchedAt},
		{Key: "upstream_status", Value: "available"},
		{Key: "updated_at", Value: fetchedAt},
	}
	if lastDataAt != nil && !lastDataAt.IsZero() {
		setFields = append(setFields,
			bson.E{Key: "last_data_at", Value: lastDataAt.UTC()},
			bson.E{Key: "data_status", Value: "fresh"},
		)
	}
	if strings.TrimSpace(observationID) != "" {
		setFields = append(setFields, bson.E{Key: "last_observation_id", Value: strings.TrimSpace(observationID)})
	}

	_, err := s.collection.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: stateDocumentID}},
		bson.D{
			{Key: "$set", Value: setFields},
			{Key: "$unset", Value: bson.D{{Key: "last_error", Value: ""}}},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("record successful Sensor Pantai fetch: %w", err)
	}
	return nil
}

// RecordSourceFailure marks the source data as stale without overwriting the
// last-known-good timestamps or observation identity.
func (s *StateStore) RecordSourceFailure(ctx context.Context, failure error, failedAt time.Time) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if failedAt.IsZero() {
		return fmt.Errorf("failedAt must not be zero")
	}

	message := "unknown upstream failure"
	if failure != nil {
		message = failure.Error()
	}
	message = truncateString(message, 1024)
	failedAt = failedAt.UTC()

	_, err := s.collection.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: stateDocumentID}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "upstream_status", Value: "unavailable"},
			{Key: "data_status", Value: "stale"},
			{Key: "last_failure_at", Value: failedAt},
			{Key: "last_error", Value: message},
			{Key: "updated_at", Value: failedAt},
		}}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("record Sensor Pantai failure: %w", err)
	}
	return nil
}

// MarkDataUnavailable records that there is no usable last-known-good reading.
// It does not delete the previously persisted timestamps or cursor.
func (s *StateStore) MarkDataUnavailable(ctx context.Context, at time.Time, reason string) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if at.IsZero() {
		return fmt.Errorf("at must not be zero")
	}

	_, err := s.collection.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: stateDocumentID}},
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "data_status", Value: "unavailable"},
			{Key: "last_error", Value: truncateString(reason, 1024)},
			{Key: "updated_at", Value: at.UTC()},
		}}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("mark Sensor Pantai data unavailable: %w", err)
	}
	return nil
}

// Close disconnects the MongoDB client. Call it during graceful shutdown.
func (s *StateStore) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}
	if err := s.client.Disconnect(ctx); err != nil {
		return fmt.Errorf("disconnect adapter state store: %w", err)
	}
	return nil
}

func (s *StateStore) ensureReady() error {
	if s == nil || s.client == nil || s.collection == nil {
		return fmt.Errorf("adapter state store is not initialized")
	}
	return nil
}

// CommitPage atomically records the page cursor and successful-fetch metadata.
// Call it only after every item in that page has been acknowledged by Kafka.
// This avoids a checkpoint being committed without its corresponding source
// status metadata (or vice versa).
func (s *StateStore) CommitPage(
	ctx context.Context,
	cursor string,
	fetchedAt time.Time,
	lastDataAt *time.Time,
	observationID string,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if fetchedAt.IsZero() {
		return fmt.Errorf("fetchedAt must not be zero")
	}

	fetchedAt = fetchedAt.UTC()
	setFields := bson.D{
		{Key: "cursor", Value: cursor},
		{Key: "checkpoint_updated_at", Value: fetchedAt},
		{Key: "last_success_at", Value: fetchedAt},
		{Key: "upstream_status", Value: "available"},
		{Key: "updated_at", Value: fetchedAt},
	}
	if lastDataAt != nil && !lastDataAt.IsZero() {
		setFields = append(setFields,
			bson.E{Key: "last_data_at", Value: lastDataAt.UTC()},
			bson.E{Key: "data_status", Value: "fresh"},
		)
	}
	if strings.TrimSpace(observationID) != "" {
		setFields = append(setFields, bson.E{Key: "last_observation_id", Value: strings.TrimSpace(observationID)})
	}

	_, err := s.collection.UpdateOne(
		ctx,
		bson.D{{Key: "_id", Value: stateDocumentID}},
		bson.D{
			{Key: "$set", Value: setFields},
			{Key: "$unset", Value: bson.D{{Key: "last_error", Value: ""}}},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("commit Sensor Pantai page checkpoint: %w", err)
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

// redactMongoURIDetails deliberately avoids returning a raw driver error that
// may contain a credential-bearing URI. Driver errors are wrapped generically.
func redactMongoURIDetails(err error) error {
	if err == nil {
		return nil
	}
	// Do not include the driver's error string: some driver/configuration errors
	// may echo a URI containing username/password credentials.
	return fmt.Errorf("MongoDB driver operation failed (%T)", err)
}
