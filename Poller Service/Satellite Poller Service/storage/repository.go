package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"satellitepoller/domain"
)

const satelliteStreamName = "satellite"

var (
	ErrInboxOffsetConflict = errors.New("feed offset already exists with different event data")
	ErrInvalidCheckpoint   = errors.New("invalid checkpoint")
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) (*Repository, error) {
	if db == nil {
		return nil, errors.New("SQLite database is nil")
	}
	return &Repository{db: db}, nil
}

// GetCheckpoint returns (checkpoint, true, nil) when the checkpoint exists.
func (r *Repository) GetCheckpoint(ctx context.Context, name string) (domain.Checkpoint, bool, error) {
	var cp domain.Checkpoint
	var updatedAt string
	err := r.db.QueryRowContext(ctx,
		`SELECT name, last_offset, updated_at FROM checkpoints WHERE name = ?`, name,
	).Scan(&cp.Name, &cp.Offset, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Checkpoint{}, false, nil
	}
	if err != nil {
		return domain.Checkpoint{}, false, fmt.Errorf("get checkpoint %q: %w", name, err)
	}
	cp.UpdatedAt, err = parseDBTime(updatedAt)
	if err != nil {
		return domain.Checkpoint{}, false, fmt.Errorf("parse checkpoint timestamp: %w", err)
	}
	return cp, true, nil
}

// SaveCheckpoint stores a checkpoint without allowing its offset to move
// backwards. It is useful for operational/admin tasks; ingestion and processing
// should normally update checkpoints in their corresponding transactions.
func (r *Repository) SaveCheckpoint(ctx context.Context, cp domain.Checkpoint) error {
	if cp.Name == "" || cp.Offset < 0 {
		return ErrInvalidCheckpoint
	}
	if cp.UpdatedAt.IsZero() {
		cp.UpdatedAt = time.Now().UTC()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO checkpoints(name, last_offset, updated_at)
		VALUES(?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			last_offset = excluded.last_offset,
			updated_at = excluded.updated_at
		WHERE excluded.last_offset >= checkpoints.last_offset
	`, cp.Name, cp.Offset, formatDBTime(cp.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save checkpoint %q: %w", cp.Name, err)
	}
	return nil
}

// SaveInboxEventAndCheckpoint atomically inserts one raw feed event and moves
// the feed checkpoint. A replay at the same offset is accepted only when the
// event type and payload are identical.
func (r *Repository) SaveInboxEventAndCheckpoint(
	ctx context.Context,
	event domain.InboxEvent,
	checkpoint domain.Checkpoint,
) error {
	if event.Offset < 0 || checkpoint.Name == "" || checkpoint.Offset != event.Offset {
		return ErrInvalidCheckpoint
	}
	if event.Type == "" {
		return errors.New("inbox event type is required")
	}
	if event.ReceivedAt.IsZero() {
		event.ReceivedAt = time.Now().UTC()
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin inbox transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO feed_inbox(
			stream_name, feed_offset, event_type, raw_payload, received_at, status
		) VALUES(?, ?, ?, ?, ?, 'pending')
		ON CONFLICT(stream_name, feed_offset) DO NOTHING
	`, satelliteStreamName, event.Offset, string(event.Type), []byte(event.RawPayload), formatDBTime(event.ReceivedAt))
	if err != nil {
		return fmt.Errorf("insert inbox event at offset %d: %w", event.Offset, err)
	}

	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect inbox insert result: %w", err)
	}
	if inserted == 0 {
		var existingType string
		var existingPayload []byte
		if err := tx.QueryRowContext(ctx, `
			SELECT event_type, raw_payload FROM feed_inbox
			WHERE stream_name = ? AND feed_offset = ?
		`, satelliteStreamName, event.Offset).Scan(&existingType, &existingPayload); err != nil {
			return fmt.Errorf("read duplicate inbox event: %w", err)
		}
		if existingType != string(event.Type) || string(existingPayload) != string(event.RawPayload) {
			return fmt.Errorf("offset %d: %w", event.Offset, ErrInboxOffsetConflict)
		}
	}

	if checkpoint.UpdatedAt.IsZero() {
		checkpoint.UpdatedAt = time.Now().UTC()
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO checkpoints(name, last_offset, updated_at)
		VALUES(?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			last_offset = excluded.last_offset,
			updated_at = excluded.updated_at
		WHERE excluded.last_offset >= checkpoints.last_offset
	`, checkpoint.Name, checkpoint.Offset, formatDBTime(checkpoint.UpdatedAt))
	if err != nil {
		return fmt.Errorf("update feed checkpoint: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit inbox transaction: %w", err)
	}
	return nil
}

// ListInboxAfter returns unprocessed inbox events with offsets greater than
// afterOffset, in ascending offset order.
func (r *Repository) ListInboxAfter(ctx context.Context, afterOffset int64, limit int) ([]domain.InboxEvent, error) {
	if limit <= 0 {
		return nil, errors.New("inbox limit must be positive")
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT feed_offset, event_type, raw_payload, received_at
		FROM feed_inbox
		WHERE stream_name = ? AND feed_offset > ? AND status = 'pending'
		ORDER BY feed_offset ASC
		LIMIT ?
	`, satelliteStreamName, afterOffset, limit)
	if err != nil {
		return nil, fmt.Errorf("list inbox events: %w", err)
	}
	defer rows.Close()

	events := make([]domain.InboxEvent, 0)
	for rows.Next() {
		var event domain.InboxEvent
		var eventType, receivedAt string
		var payload []byte
		if err := rows.Scan(&event.Offset, &eventType, &payload, &receivedAt); err != nil {
			return nil, fmt.Errorf("scan inbox event: %w", err)
		}
		event.Type = domain.EventType(eventType)
		event.RawPayload = append(json.RawMessage(nil), payload...)
		event.ReceivedAt, err = parseDBTime(receivedAt)
		if err != nil {
			return nil, fmt.Errorf("parse inbox received_at: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inbox events: %w", err)
	}
	return events, nil
}

// GetPendingInboxEvents is a descriptive alias for ListInboxAfter.
func (r *Repository) GetPendingInboxEvents(ctx context.Context, afterOffset int64, limit int) ([]domain.InboxEvent, error) {
	return r.ListInboxAfter(ctx, afterOffset, limit)
}

// LoadDetections loads the current normalized detection snapshot.
func (r *Repository) LoadDetections(ctx context.Context) ([]domain.Detection, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT detection_key, satellite, acq_date, acq_time, event_time,
		       latitude_raw, longitude_raw, latitude, longitude, confidence,
		       frp_mw, status, raw_payload, received_at
		FROM detections ORDER BY detection_key
	`)
	if err != nil {
		return nil, fmt.Errorf("load detections: %w", err)
	}
	defer rows.Close()

	items := make([]domain.Detection, 0)
	for rows.Next() {
		var d domain.Detection
		var eventTime, receivedAt, status string
		var payload []byte
		if err := rows.Scan(
			&d.Key, &d.Satellite, &d.AcqDate, &d.AcqTime, &eventTime,
			&d.LatitudeRaw, &d.LongitudeRaw, &d.Latitude, &d.Longitude,
			&d.Confidence, &d.FRPMW, &status, &payload, &receivedAt,
		); err != nil {
			return nil, fmt.Errorf("scan detection: %w", err)
		}
		d.EventTime, err = parseDBTime(eventTime)
		if err != nil {
			return nil, fmt.Errorf("parse detection event_time: %w", err)
		}
		d.ReceivedAt, err = parseDBTime(receivedAt)
		if err != nil {
			return nil, fmt.Errorf("parse detection received_at: %w", err)
		}
		d.Status = domain.DetectionStatus(status)
		d.RawPayload = append(json.RawMessage(nil), payload...)
		items = append(items, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate detections: %w", err)
	}
	return items, nil
}

// LoadClusters loads active and historical cluster snapshots, including their
// detection memberships.
func (r *Repository) LoadClusters(ctx context.Context) ([]domain.FireCluster, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT cluster_id, anchor_key, source_ref_id, status, latitude, longitude, occurred_at,
		       detection_count, total_frp_mw, severity, satellites_json,
		       max_confidence, detection_keys_json, revision, updated_at
		FROM fire_clusters ORDER BY cluster_id
	`)
	if err != nil {
		return nil, fmt.Errorf("load clusters: %w", err)
	}
	defer rows.Close()

	items := make([]domain.FireCluster, 0)
	for rows.Next() {
		var c domain.FireCluster
		var status, occurredAt, updatedAt string
		var satellitesJSON, keysJSON []byte
		if err := rows.Scan(
			&c.ID, &c.AnchorKey, &c.SourceRefID, &status, &c.Latitude, &c.Longitude,
			&occurredAt, &c.DetectionCount, &c.TotalFRPMW, &c.Severity,
			&satellitesJSON, &c.MaxConfidence, &keysJSON, &c.Revision, &updatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan cluster: %w", err)
		}
		c.Status = domain.ClusterStatus(status)
		c.OccurredAt, err = parseDBTime(occurredAt)
		if err != nil {
			return nil, fmt.Errorf("parse cluster occurred_at: %w", err)
		}
		c.UpdatedAt, err = parseDBTime(updatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse cluster updated_at: %w", err)
		}
		if err := json.Unmarshal(satellitesJSON, &c.Satellites); err != nil {
			return nil, fmt.Errorf("decode cluster satellites: %w", err)
		}
		if err := json.Unmarshal(keysJSON, &c.DetectionKeys); err != nil {
			return nil, fmt.Errorf("decode cluster detection keys: %w", err)
		}
		if c.AnchorKey == "" && len(c.DetectionKeys) > 0 {
			sort.Strings(c.DetectionKeys)
			c.AnchorKey = c.DetectionKeys[0]
		}
		items = append(items, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate clusters: %w", err)
	}
	return items, nil
}

// CommitProcessing atomically commits the processor's full detection and
// cluster snapshots, outbox rows, inbox status updates, and processing offset.
func (r *Repository) CommitProcessing(
	ctx context.Context,
	checkpoint domain.Checkpoint,
	detections []domain.Detection,
	clusters []domain.FireCluster,
	outbox []domain.OutboxEvent,
) error {
	if checkpoint.Name == "" || checkpoint.Offset < 0 {
		return ErrInvalidCheckpoint
	}
	if checkpoint.UpdatedAt.IsZero() {
		checkpoint.UpdatedAt = time.Now().UTC()
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin processing transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, d := range detections {
		if err := upsertDetection(ctx, tx, d); err != nil {
			return err
		}
	}

	for _, c := range clusters {
		if err := upsertCluster(ctx, tx, c); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM cluster_members"); err != nil {
		return fmt.Errorf("replace cluster memberships: %w", err)
	}
	memberStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO cluster_members(cluster_id, detection_key) VALUES(?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare cluster membership insert: %w", err)
	}
	for _, c := range clusters {
		if c.Status != domain.ClusterActive {
			continue
		}
		for _, key := range c.DetectionKeys {
			if _, err := memberStmt.ExecContext(ctx, c.ID, key); err != nil {
				_ = memberStmt.Close()
				return fmt.Errorf("insert cluster member %q into %q: %w", key, c.ID, err)
			}
		}
	}
	if err := memberStmt.Close(); err != nil {
		return fmt.Errorf("close cluster membership statement: %w", err)
	}

	for _, event := range outbox {
		if err := insertOutbox(ctx, tx, event); err != nil {
			return err
		}
	}

	now := formatDBTime(time.Now().UTC())
	if _, err := tx.ExecContext(ctx, `
		UPDATE feed_inbox
		SET status = 'processed', processed_at = ?, last_error = NULL
		WHERE stream_name = ? AND feed_offset <= ? AND status <> 'processed'
	`, now, satelliteStreamName, checkpoint.Offset); err != nil {
		return fmt.Errorf("mark inbox events processed: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO checkpoints(name, last_offset, updated_at)
		VALUES(?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			last_offset = excluded.last_offset,
			updated_at = excluded.updated_at
		WHERE excluded.last_offset >= checkpoints.last_offset
	`, checkpoint.Name, checkpoint.Offset, formatDBTime(checkpoint.UpdatedAt)); err != nil {
		return fmt.Errorf("save processing checkpoint: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit processing transaction: %w", err)
	}
	return nil
}

func upsertDetection(ctx context.Context, tx *sql.Tx, d domain.Detection) error {
	if d.Key == "" {
		return errors.New("detection key is required")
	}
	if d.EventTime.IsZero() || d.ReceivedAt.IsZero() {
		return fmt.Errorf("detection %q has zero timestamp", d.Key)
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO detections(
			detection_key, satellite, acq_date, acq_time, event_time,
			latitude_raw, longitude_raw, latitude, longitude, confidence,
			frp_mw, status, raw_payload, received_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(detection_key) DO UPDATE SET
			satellite = excluded.satellite,
			acq_date = excluded.acq_date,
			acq_time = excluded.acq_time,
			event_time = excluded.event_time,
			latitude_raw = excluded.latitude_raw,
			longitude_raw = excluded.longitude_raw,
			latitude = excluded.latitude,
			longitude = excluded.longitude,
			confidence = excluded.confidence,
			frp_mw = excluded.frp_mw,
			status = excluded.status,
			raw_payload = excluded.raw_payload,
			received_at = excluded.received_at
	`, d.Key, d.Satellite, d.AcqDate, d.AcqTime, formatDBTime(d.EventTime),
		d.LatitudeRaw, d.LongitudeRaw, d.Latitude, d.Longitude, d.Confidence,
		d.FRPMW, string(d.Status), []byte(d.RawPayload), formatDBTime(d.ReceivedAt))
	if err != nil {
		return fmt.Errorf("upsert detection %q: %w", d.Key, err)
	}
	return nil
}

func upsertCluster(ctx context.Context, tx *sql.Tx, c domain.FireCluster) error {
	if c.ID == "" || c.SourceRefID == "" {
		return errors.New("cluster ID and source_ref_id are required")
	}
	satellites, err := json.Marshal(c.Satellites)
	if err != nil {
		return fmt.Errorf("encode cluster satellites: %w", err)
	}
	keys, err := json.Marshal(c.DetectionKeys)
	if err != nil {
		return fmt.Errorf("encode cluster detection keys: %w", err)
	}
	if c.OccurredAt.IsZero() || c.UpdatedAt.IsZero() {
		return fmt.Errorf("cluster %q has zero timestamp", c.ID)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO fire_clusters(
			cluster_id, anchor_key, source_ref_id, status, latitude, longitude, occurred_at,
			detection_count, total_frp_mw, severity, satellites_json,
			max_confidence, detection_keys_json, revision, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(cluster_id) DO UPDATE SET
			anchor_key = excluded.anchor_key,
			source_ref_id = excluded.source_ref_id,
			status = excluded.status,
			latitude = excluded.latitude,
			longitude = excluded.longitude,
			occurred_at = excluded.occurred_at,
			detection_count = excluded.detection_count,
			total_frp_mw = excluded.total_frp_mw,
			severity = excluded.severity,
			satellites_json = excluded.satellites_json,
			max_confidence = excluded.max_confidence,
			detection_keys_json = excluded.detection_keys_json,
			revision = excluded.revision,
			updated_at = excluded.updated_at
	`, c.ID, c.SourceRefID, string(c.Status), c.Latitude, c.Longitude,
		formatDBTime(c.OccurredAt), c.DetectionCount, c.TotalFRPMW, c.Severity,
		satellites, c.MaxConfidence, keys, c.Revision, formatDBTime(c.UpdatedAt))
	if err != nil {
		return fmt.Errorf("upsert cluster %q: %w", c.ID, err)
	}
	return nil
}

func insertOutbox(ctx context.Context, tx *sql.Tx, event domain.OutboxEvent) error {
	if event.EventID == "" || event.SourceRefID == "" || len(event.Payload) == 0 {
		return errors.New("outbox event requires event ID, source_ref_id, and payload")
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	var publishedAt any
	if event.PublishedAt != nil {
		publishedAt = formatDBTime(*event.PublishedAt)
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO outbox_events(
			event_id, source_ref_id, revision, payload, status,
			attempts, created_at, published_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id) DO NOTHING
	`, event.EventID, event.SourceRefID, event.Revision, []byte(event.Payload),
		normalizeOutboxStatus(event.Status), event.Attempts,
		formatDBTime(event.CreatedAt), publishedAt)
	if err != nil {
		return fmt.Errorf("insert outbox event %q: %w", event.EventID, err)
	}
	return nil
}

// SaveOutboxEvent stores one outbox event idempotently. Normal processing uses
// CommitProcessing so its outbox insertion is atomic with the state changes.
func (r *Repository) SaveOutboxEvent(ctx context.Context, event domain.OutboxEvent) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin outbox transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := insertOutbox(ctx, tx, event); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit outbox event: %w", err)
	}
	return nil
}

// GetPendingOutboxEvents returns unpublished events in creation order.
func (r *Repository) GetPendingOutboxEvents(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	if limit <= 0 {
		return nil, errors.New("outbox limit must be positive")
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT event_id, source_ref_id, revision, payload, status,
		       attempts, created_at, published_at
		FROM outbox_events
		WHERE status = 'pending'
		ORDER BY source_ref_id ASC,
					revision ASC,
					created_at ASC,
					event_id ASC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox events: %w", err)
	}
	defer rows.Close()

	items := make([]domain.OutboxEvent, 0)
	for rows.Next() {
		var item domain.OutboxEvent
		var payload []byte
		var createdAt string
		var publishedAt sql.NullString
		if err := rows.Scan(
			&item.EventID, &item.SourceRefID, &item.Revision, &payload,
			&item.Status, &item.Attempts, &createdAt, &publishedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		item.Payload = append(json.RawMessage(nil), payload...)
		item.CreatedAt, err = parseDBTime(createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse outbox created_at: %w", err)
		}
		if publishedAt.Valid {
			value, err := parseDBTime(publishedAt.String)
			if err != nil {
				return nil, fmt.Errorf("parse outbox published_at: %w", err)
			}
			item.PublishedAt = &value
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox events: %w", err)
	}
	return items, nil
}

// MarkOutboxPublished marks an event as published after the Kafka broker ACKs it.
func (r *Repository) MarkOutboxPublished(ctx context.Context, eventID string, publishedAt time.Time) error {
	if publishedAt.IsZero() {
		publishedAt = time.Now().UTC()
	}
	result, err := r.db.ExecContext(ctx, `
		UPDATE outbox_events
		SET status = 'published', published_at = ?, last_error = NULL
		WHERE event_id = ?
	`, formatDBTime(publishedAt), eventID)
	if err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// MarkOutboxFailed records a failed publish attempt but leaves the event
// pending so the publisher can retry it later.
func (r *Repository) MarkOutboxFailed(ctx context.Context, eventID, cause string) error {
	result, err := r.db.ExecContext(ctx, `
		UPDATE outbox_events
		SET attempts = attempts + 1, last_error = ?, status = 'pending'
		WHERE event_id = ?
	`, cause, eventID)
	if err != nil {
		return fmt.Errorf("record outbox publish failure: %w", err)
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// OutboxPendingCount is useful for health and backlog metrics.
func (r *Repository) OutboxPendingCount(ctx context.Context) (int64, error) {
	var count int64
	if err := r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM outbox_events WHERE status = 'pending'",
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending outbox events: %w", err)
	}
	return count, nil
}

func normalizeOutboxStatus(status string) string {
	if status == "" {
		return "pending"
	}
	return status
}

func formatDBTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseDBTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}
