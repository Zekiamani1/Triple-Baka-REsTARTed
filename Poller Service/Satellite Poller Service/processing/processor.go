package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"satellitepoller/domain"
)

const (
	ProcessingCheckpointName = "satellite-processing"
	defaultBatchSize         = 100
	defaultPollInterval      = 250 * time.Millisecond
)

var (
	ErrUnsupportedEvent = errors.New("unsupported satellite event type")
	ErrMalformedPayload = errors.New("malformed satellite event payload")
)

type Repository interface {
	GetCheckpoint(ctx context.Context, name string) (domain.Checkpoint, bool, error)
	ListInboxAfter(ctx context.Context, offset int64, limit int) ([]domain.InboxEvent, error)
	LoadDetections(ctx context.Context) ([]domain.Detection, error)
	LoadClusters(ctx context.Context) ([]domain.FireCluster, error)
	CommitProcessing(
		ctx context.Context,
		checkpoint domain.Checkpoint,
		detections []domain.Detection,
		clusters []domain.FireCluster,
		outbox []domain.OutboxEvent,
	) error
}

type Options struct {
	BatchSize    int
	PollInterval time.Duration
	MaxLateness  time.Duration
	Logger       *slog.Logger
}

type Stats struct {
	ProcessedEvents    uint64
	DuplicateEvents    uint64
	FilteredDetections uint64
	LateDetections     uint64
	ActiveClusters     uint64
	SingleDetections   uint64
}

type counters struct {
	processed        atomic.Uint64
	duplicates       atomic.Uint64
	filtered         atomic.Uint64
	late             atomic.Uint64
	activeClusters   atomic.Uint64
	singleDetections atomic.Uint64
}

type Processor struct {
	repo   Repository
	opts   Options
	logger *slog.Logger
	stats  counters
}

func NewProcessor(repo Repository, opts Options) (*Processor, error) {
	if repo == nil {
		return nil, errors.New("processing repository is required")
	}
	if opts.MaxLateness <= 0 {
		return nil, errors.New("max lateness must be positive")
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
	return &Processor{repo: repo, opts: opts, logger: opts.Logger.With("component", "satellite-processor")}, nil
}

// Run continuously processes inbox events until ctx is cancelled.
func (p *Processor) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.opts.PollInterval)
	defer ticker.Stop()

	for {
		processed, err := p.ProcessAvailable(ctx)
		if err != nil {
			return err
		}
		if processed > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// ProcessAvailable processes up to BatchSize inbox events in ascending offset
// order. It loads the state once, applies the batch, recomputes clustering once,
// then commits the full state and the final processing offset atomically.
func (p *Processor) ProcessAvailable(ctx context.Context) (int, error) {
	cp, found, err := p.repo.GetCheckpoint(ctx, ProcessingCheckpointName)
	if err != nil {
		return 0, fmt.Errorf("load processing checkpoint: %w", err)
	}
	after := int64(-1)
	if found {
		after = cp.Offset
	}

	events, err := p.repo.ListInboxAfter(ctx, after, p.opts.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("list pending inbox events: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Offset < events[j].Offset })
	for i := 1; i < len(events); i++ {
		if events[i].Offset != events[i-1].Offset+1 {
			return 0, fmt.Errorf("processing inbox offset gap: previous=%d current=%d", events[i-1].Offset, events[i].Offset)
		}
	}
	if found && events[0].Offset != after+1 {
		return 0, fmt.Errorf("processing inbox offset gap: checkpoint=%d first_pending=%d", after, events[0].Offset)
	}

	detections, err := p.repo.LoadDetections(ctx)
	if err != nil {
		return 0, fmt.Errorf("load detections: %w", err)
	}
	oldClusters, err := p.repo.LoadClusters(ctx)
	if err != nil {
		return 0, fmt.Errorf("load clusters: %w", err)
	}
	detectionMap := make(map[string]domain.Detection, len(detections)+len(events))
	for _, d := range detections {
		detectionMap[d.Key] = d
	}

	var duplicates, filtered, late uint64
	changed := false
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return 0, nil
		}
		itemChanged, duplicate, itemFiltered, itemLate, err := p.applyEvent(event, detectionMap)
		if err != nil {
			return 0, fmt.Errorf("offset %d (%s): %w", event.Offset, event.Type, err)
		}
		if itemChanged {
			changed = true
		}
		if duplicate {
			duplicates++
		}
		if itemFiltered {
			filtered++
		}
		if itemLate {
			late++
		}
	}

	allDetections := make([]domain.Detection, 0, len(detectionMap))
	for _, d := range detectionMap {
		allDetections = append(allDetections, d)
	}
	sort.Slice(allDetections, func(i, j int) bool { return allDetections[i].Key < allDetections[j].Key })

	clusters := oldClusters
	var outbox []domain.OutboxEvent
	singleCount := 0
	if changed {
		computed, singles := BuildClusters(allDetections, time.Now().UTC())
		singleCount = singles
		clusters, outbox, err = ReconcileClusters(oldClusters, computed, time.Now().UTC())
		if err != nil {
			return 0, fmt.Errorf("reconcile fire clusters: %w", err)
		}
	}

	lastOffset := events[len(events)-1].Offset
	checkpoint := domain.Checkpoint{Name: ProcessingCheckpointName, Offset: lastOffset, UpdatedAt: time.Now().UTC()}
	if err := p.repo.CommitProcessing(ctx, checkpoint, allDetections, clusters, outbox); err != nil {
		return 0, fmt.Errorf("commit processing batch through offset %d: %w", lastOffset, err)
	}

	p.stats.processed.Add(uint64(len(events)))
	p.stats.duplicates.Add(duplicates)
	p.stats.filtered.Add(filtered)
	p.stats.late.Add(late)
	if changed {
		active := 0
		for _, cluster := range clusters {
			if cluster.Status == domain.ClusterActive {
				active++
			}
		}
		p.stats.activeClusters.Store(uint64(active))
		p.stats.singleDetections.Store(uint64(singleCount))
	}
	p.logger.Debug("satellite inbox batch processed",
		"first_offset", events[0].Offset,
		"last_offset", lastOffset,
		"event_count", len(events),
		"detection_count", len(allDetections),
		"outbox_events", len(outbox),
	)
	return len(events), nil
}

func (p *Processor) applyEvent(event domain.InboxEvent, detections map[string]domain.Detection) (changed, duplicate, filtered, late bool, err error) {
	switch event.Type {
	case domain.EventHeartbeat:
		return false, false, false, false, nil

	case domain.EventDetection:
		d, err := decodeDetection(event.RawPayload, event.ReceivedAt)
		if err != nil {
			return false, false, false, false, err
		}
		if _, exists := detections[d.Key]; exists {
			return false, true, false, false, nil
		}
		d.Status, filtered, late = p.classifyDetection(d, detections)
		detections[d.Key] = d
		return true, false, filtered, late, nil

	case domain.EventUpdate:
		key, err := updateReferenceKey(event.RawPayload)
		if err != nil {
			return false, false, false, false, err
		}
		old, exists := detections[key]
		var updated domain.Detection
		if exists {
			updated, err = applyDetectionPatch(old, event.RawPayload, event.ReceivedAt)
		} else {
			// If the feed replays an update before the original detection, it must
			// carry a complete detection object so the state can be reconstructed.
			updated, err = decodeDetection(event.RawPayload, event.ReceivedAt)
		}
		if err != nil {
			return false, false, false, false, fmt.Errorf("apply detection update %q: %w", key, err)
		}
		if exists && updated.Key != key {
			old.Status = domain.DetectionRetracted
			detections[key] = old
		}
		updated.Status, filtered, late = p.classifyDetection(updated, detections)
		detections[updated.Key] = updated
		return true, false, filtered, late, nil

	case domain.EventRetract:
		key, err := decodeDetectionKey(event.RawPayload)
		if err != nil {
			return false, false, false, false, err
		}
		d, exists := detections[key]
		if !exists || d.Status == domain.DetectionRetracted {
			return false, true, false, false, nil
		}
		d.Status = domain.DetectionRetracted
		d.ReceivedAt = event.ReceivedAt
		detections[key] = d
		return true, false, false, false, nil

	default:
		return false, false, false, false, fmt.Errorf("%w: %q", ErrUnsupportedEvent, event.Type)
	}
}

func (p *Processor) classifyDetection(d domain.Detection, existing map[string]domain.Detection) (domain.DetectionStatus, bool, bool) {
	if isLowConfidence(d.Confidence) {
		return domain.DetectionFiltered, true, false
	}
	maxSeen := time.Time{}
	for _, old := range existing {
		if old.EventTime.After(maxSeen) {
			maxSeen = old.EventTime
		}
	}
	if !maxSeen.IsZero() && d.EventTime.Before(maxSeen.Add(-p.opts.MaxLateness)) {
		return domain.DetectionLate, false, true
	}
	return domain.DetectionActive, false, false
}

// Snapshot returns counters accumulated since this process started.
func (p *Processor) Snapshot() Stats {
	return Stats{
		ProcessedEvents:    p.stats.processed.Load(),
		DuplicateEvents:    p.stats.duplicates.Load(),
		FilteredDetections: p.stats.filtered.Load(),
		LateDetections:     p.stats.late.Load(),
		ActiveClusters:     p.stats.activeClusters.Load(),
		SingleDetections:   p.stats.singleDetections.Load(),
	}
}

func decodeDetection(raw json.RawMessage, receivedAt time.Time) (domain.Detection, error) {
	obj, err := decodePayloadObject(raw)
	if err != nil {
		return domain.Detection{}, err
	}
	if nested, ok := obj["detection"]; ok {
		nestedObj, err := decodePayloadObject(nested)
		if err != nil {
			return domain.Detection{}, err
		}
		obj = nestedObj
	}

	satellite, err := requiredString(obj, "satellite")
	if err != nil {
		return domain.Detection{}, err
	}
	acqDate, err := requiredString(obj, "acq_date")
	if err != nil {
		return domain.Detection{}, err
	}
	acqTime, err := requiredString(obj, "acq_time")
	if err != nil {
		return domain.Detection{}, err
	}
	latRaw, lat, err := requiredNumber(obj, "latitude")
	if err != nil {
		return domain.Detection{}, err
	}
	lonRaw, lon, err := requiredNumber(obj, "longitude")
	if err != nil {
		return domain.Detection{}, err
	}
	confidence, err := requiredString(obj, "confidence")
	if err != nil {
		return domain.Detection{}, err
	}
	_, frp, err := optionalNumber(obj, "frp", "frp_mw")
	if err != nil {
		return domain.Detection{}, err
	}

	eventTime, err := parseAcquisitionTime(acqDate, acqTime)
	if err != nil {
		return domain.Detection{}, err
	}
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return domain.Detection{}, fmt.Errorf("%w: latitude/longitude out of range", ErrMalformedPayload)
	}
	if frp < 0 {
		return domain.Detection{}, fmt.Errorf("%w: FRP must not be negative", ErrMalformedPayload)
	}

	key := strings.Join([]string{satellite, acqDate, acqTime, latRaw, lonRaw}, "|")
	return domain.Detection{
		Key: key, Satellite: satellite, AcqDate: acqDate, AcqTime: acqTime,
		EventTime: eventTime, LatitudeRaw: latRaw, LongitudeRaw: lonRaw,
		Latitude: lat, Longitude: lon, Confidence: strings.ToLower(confidence),
		FRPMW: frp, Status: domain.DetectionActive,
		RawPayload: append(json.RawMessage(nil), raw...), ReceivedAt: receivedAt.UTC(),
	}, nil
}

func updateReferenceKey(raw json.RawMessage) (string, error) {
	obj, err := decodePayloadObject(raw)
	if err != nil {
		return "", err
	}
	if nested, ok := obj["detection"]; ok {
		if nestedObj, decodeErr := decodePayloadObject(nested); decodeErr == nil {
			if key := firstString(nestedObj, "detection_key", "key"); key != "" {
				return key, nil
			}
		}
	}
	if key := firstString(obj, "detection_key", "previous_detection_key", "key"); key != "" {
		return key, nil
	}
	// A full update can identify itself by its natural detection key.
	d, err := decodeDetection(raw, time.Time{})
	if err != nil {
		return "", fmt.Errorf("%w: update must include a detection key or full detection identity", ErrMalformedPayload)
	}
	return d.Key, nil
}

func applyDetectionPatch(old domain.Detection, raw json.RawMessage, receivedAt time.Time) (domain.Detection, error) {
	base, err := decodePayloadObject(old.RawPayload)
	if err != nil {
		return domain.Detection{}, fmt.Errorf("decode stored detection: %w", err)
	}
	if nested, ok := base["detection"]; ok {
		base, err = decodePayloadObject(nested)
		if err != nil {
			return domain.Detection{}, err
		}
	}
	patch, err := decodePayloadObject(raw)
	if err != nil {
		return domain.Detection{}, err
	}
	if nested, ok := patch["detection"]; ok {
		patch, err = decodePayloadObject(nested)
		if err != nil {
			return domain.Detection{}, err
		}
	}
	for key, value := range patch {
		switch key {
		case "detection_key", "previous_detection_key", "key":
			continue
		default:
			base[key] = value
		}
	}
	merged, err := json.Marshal(base)
	if err != nil {
		return domain.Detection{}, fmt.Errorf("merge detection update: %w", err)
	}
	return decodeDetection(merged, receivedAt)
}

func decodeDetectionKey(raw json.RawMessage) (string, error) {
	obj, err := decodePayloadObject(raw)
	if err != nil {
		return "", err
	}
	if key := firstString(obj, "detection_key", "previous_detection_key", "key"); key != "" {
		return key, nil
	}
	d, err := decodeDetection(raw, time.Now().UTC())
	if err != nil {
		return "", fmt.Errorf("retract must contain detection_key or detection identity: %w", err)
	}
	return d.Key, nil
}

func decodePayloadObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if len(raw) == 0 || string(raw) == "null" {
		return nil, fmt.Errorf("%w: empty JSON payload", ErrMalformedPayload)
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedPayload, err)
	}
	if obj == nil {
		return nil, fmt.Errorf("%w: payload must be a JSON object", ErrMalformedPayload)
	}
	return obj, nil
}

func requiredString(obj map[string]json.RawMessage, field string) (string, error) {
	value := rawString(obj[field])
	if value == "" {
		return "", fmt.Errorf("%w: missing field %q", ErrMalformedPayload, field)
	}
	return value, nil
}

func firstString(obj map[string]json.RawMessage, fields ...string) string {
	for _, field := range fields {
		if value := rawString(obj[field]); value != "" {
			return value
		}
	}
	return ""
}

func rawString(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return strings.TrimSpace(s)
	}
	return strings.TrimSpace(string(raw))
}

func requiredNumber(obj map[string]json.RawMessage, field string) (string, float64, error) {
	raw := rawString(obj[field])
	if raw == "" {
		return "", 0, fmt.Errorf("%w: missing numeric field %q", ErrMalformedPayload, field)
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return "", 0, fmt.Errorf("%w: field %q: %v", ErrMalformedPayload, field, err)
	}
	return raw, value, nil
}

func optionalNumber(obj map[string]json.RawMessage, fields ...string) (string, float64, error) {
	for _, field := range fields {
		if len(obj[field]) == 0 || string(obj[field]) == "null" {
			continue
		}
		return requiredNumber(obj, field)
	}
	return "0", 0, nil
}

func parseAcquisitionTime(dateValue, timeValue string) (time.Time, error) {
	date, err := time.Parse("2006-01-02", dateValue)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid acq_date %q", ErrMalformedPayload, dateValue)
	}
	t := strings.TrimSpace(timeValue)
	var hour, minute, second int
	if strings.Contains(t, ":") {
		parsed, err := time.Parse("15:04:05", t)
		if err != nil {
			parsed, err = time.Parse("15:04", t)
			if err != nil {
				return time.Time{}, fmt.Errorf("%w: invalid acq_time %q", ErrMalformedPayload, timeValue)
			}
		}
		hour, minute, second = parsed.Hour(), parsed.Minute(), parsed.Second()
	} else {
		if t == "" || len(t) > 4 {
			return time.Time{}, fmt.Errorf("%w: invalid acq_time %q; expected HHMM", ErrMalformedPayload, timeValue)
		}
		for len(t) < 4 {
			t = "0" + t
		}
		var parseErr error
		hour, parseErr = strconv.Atoi(t[:2])
		if parseErr != nil {
			return time.Time{}, fmt.Errorf("%w: invalid acq_time %q", ErrMalformedPayload, timeValue)
		}
		minute, parseErr = strconv.Atoi(t[2:])
		if parseErr != nil {
			return time.Time{}, fmt.Errorf("%w: invalid acq_time %q", ErrMalformedPayload, timeValue)
		}
	}
	if hour > 23 || minute > 59 || second > 59 {
		return time.Time{}, fmt.Errorf("%w: acq_time out of range %q", ErrMalformedPayload, timeValue)
	}
	return time.Date(date.Year(), date.Month(), date.Day(), hour, minute, second, 0, time.UTC), nil
}
