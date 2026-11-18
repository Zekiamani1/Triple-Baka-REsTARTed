// Package domain contains the internal data models used by the Satellite Poller.
// These models are independent of the SSE, SQLite, and Kafka representations.
package domain

import (
	"encoding/json"
	"time"
)

// EventType represents an event type emitted by the Satellite Feed.
type EventType string

const (
	EventDetection EventType = "detection"
	EventUpdate    EventType = "update"
	EventRetract   EventType = "retract"
	EventHeartbeat EventType = "heartbeat"
)

// InboxEvent is the durable representation of one event received from the feed.
// RawPayload is retained so the event can be replayed and audited.
type InboxEvent struct {
	Offset     int64
	Type       EventType
	RawPayload json.RawMessage
	ReceivedAt time.Time
}

// DetectionStatus describes whether a detection is active or has been retracted.
type DetectionStatus string

const (
	DetectionActive    DetectionStatus = "active"
	DetectionRetracted DetectionStatus = "retracted"
	DetectionFiltered  DetectionStatus = "filtered"
	DetectionLate      DetectionStatus = "late"
)

// Detection represents a normalized satellite hotspot detection.
// LatitudeRaw and LongitudeRaw preserve the original feed values for the
// detection key; Latitude and Longitude are used for geographic calculations.
type Detection struct {
	Key          string
	Satellite    string
	AcqDate      string
	AcqTime      string
	EventTime    time.Time
	LatitudeRaw  string
	LongitudeRaw string
	Latitude     float64
	Longitude    float64
	Confidence   string
	FRPMW        float64
	Status       DetectionStatus
	RawPayload   json.RawMessage
	ReceivedAt   time.Time
}

// ClusterStatus describes the lifecycle of a fire cluster.
type ClusterStatus string

const (
	ClusterActive     ClusterStatus = "active"
	ClusterSuperseded ClusterStatus = "superseded"
	ClusterRetracted  ClusterStatus = "retracted"
)

// FireCluster represents a connected component of active detections.
type FireCluster struct {
	ID             string
	SourceRefID    string
	Status         ClusterStatus
	Latitude       float64
	Longitude      float64
	OccurredAt     time.Time
	DetectionCount int
	TotalFRPMW     float64
	Severity       string
	Satellites     []string
	MaxConfidence  string
	DetectionKeys  []string
	Revision       int64
	UpdatedAt      time.Time
}

// HazardEvent is the internal, transport-independent representation of the
// canonical HazardEvent published to Kafka. The publisher maps it to Protobuf.
type HazardEvent struct {
	HazardID    string
	Source      string
	SourceRefID string
	HazardType  string
	Severity    string
	AreaName    string
	Latitude    float64
	Longitude   float64
	OccurredAt  time.Time
	IngestedAt  time.Time
	Attributes  map[string]any
}

// Checkpoint stores a durable progress marker. Keep feed ingestion and event
// processing checkpoints separate by using different Name values.
type Checkpoint struct {
	Name      string
	Offset    int64
	UpdatedAt time.Time
}

// OutboxEvent stores a HazardEvent awaiting publication or a prior publication
// attempt. EventID must be deterministic for a given source_ref_id and revision.
type OutboxEvent struct {
	EventID     string
	SourceRefID string
	Revision    int64
	Payload     json.RawMessage
	Status      string
	Attempts    int
	CreatedAt   time.Time
	PublishedAt *time.Time
}
