package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const (
	validationMaxAgeEnv     = "MAX_OBSERVATION_AGE_MS"
	validationFutureEnv     = "MAX_FUTURE_SKEW_MS"
	stateOperationTimeout   = 5 * time.Second
	shutdownWaitTimeout     = 15 * time.Second
	retryableBatchLimit     = 100
	initialRetryBackoff     = 5 * time.Second
	maximumRetryBackoff     = 5 * time.Second
	maxDurationMilliseconds = int64((1<<63 - 1) / int64(time.Millisecond))
)

type CoastalAdapter struct {
	config    Config
	policy    ValidationPolicy
	sensor    *SensorClient
	state     *StateStore
	publisher *KafkaPublisher
	telemetry *Telemetry
	logger    *slog.Logger
	stations  map[string]Station
}

func main() {
	if err := run(); err != nil {
		slog.Error("Sensor Pantai adapter stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	config, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	policy, err := loadValidationPolicyFromEnv()
	if err != nil {
		return fmt.Errorf("load validation policy: %w", err)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, startupCancel := context.WithTimeout(rootCtx, 15*time.Second)
	telemetry, err := NewTelemetry(startupCtx, config)
	startupCancel()
	if err != nil {
		return fmt.Errorf("initialize telemetry: %w", err)
	}
	dependencyCleanupAllowed := true
	defer func() {
		if !dependencyCleanupAllowed {
			logger.Error("skipping dependency cleanup because the polling goroutine did not stop; process exit will release resources")
			return
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := telemetry.Shutdown(shutdownCtx); err != nil {
			logger.Warn("telemetry shutdown failed", "error", err)
		}
	}()

	stateCtx, stateCancel := context.WithTimeout(rootCtx, 10*time.Second)
	state, err := NewStateStore(stateCtx, config.PantaiStateURI)
	stateCancel()
	if err != nil {
		return fmt.Errorf("initialize durable state store: %w", err)
	}
	defer func() {
		if !dependencyCleanupAllowed {
			return
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := state.Close(closeCtx); err != nil {
			logger.Warn("state store shutdown failed", "error", err)
		}
	}()

	publisher, err := NewKafkaPublisher(config)
	if err != nil {
		return fmt.Errorf("initialize Kafka publisher: %w", err)
	}
	defer func() {
		if !dependencyCleanupAllowed {
			return
		}
		if err := publisher.Close(); err != nil {
			logger.Warn("Kafka publisher shutdown failed", "error", err)
		}
	}()

	sensor, err := NewSensorClient(config)
	if err != nil {
		return fmt.Errorf("initialize Sensor Pantai client: %w", err)
	}
	telemetry.SetCircuitBreakerSource(sensor.BreakerState)

	loadCtx, loadCancel := context.WithTimeout(rootCtx, stateOperationTimeout)
	cursor, err := state.LoadCheckpoint(loadCtx)
	if err != nil {
		loadCancel()
		return fmt.Errorf("load Sensor Pantai checkpoint: %w", err)
	}
	if err := state.RefreshDataStatus(loadCtx, time.Now().UTC(), policy.MaxObservationAge); err != nil {
		loadCancel()
		return fmt.Errorf("refresh persisted Sensor Pantai data freshness: %w", err)
	}
	sourceStatus, err := state.LoadSourceStatus(loadCtx)
	loadCancel()
	if err != nil {
		return fmt.Errorf("load Sensor Pantai source status: %w", err)
	}
	if sourceStatus.LastDataAt != nil {
		telemetry.SetLastDataAt(*sourceStatus.LastDataAt)
	}

	adapter := &CoastalAdapter{
		config:    config,
		policy:    policy,
		sensor:    sensor,
		state:     state,
		publisher: publisher,
		telemetry: telemetry,
		logger:    logger,
		stations:  make(map[string]Station),
	}

	var ready atomic.Bool
	ready.Store(true) // The adapter can recover an unavailable upstream after startup.
	healthServer := newHealthServer(config.HealthPort, &ready, sensor)
	healthErrors := make(chan error, 1)
	go func() {
		logger.Info("health server listening", "port", config.HealthPort)
		if err := healthServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			healthErrors <- err
		}
	}()

	logger.Info(
		"Sensor Pantai adapter started",
		"sensor_base_url", config.SensorBaseURL,
		"poll_interval", config.PollInterval.String(),
		"checkpoint_present", cursor != "",
		"last_upstream_status", sourceStatus.UpstreamStatus,
		"data_status", sourceStatus.DataStatus,
	)

	loopErr := make(chan error, 1)
	go func() {
		loopErr <- adapter.Run(rootCtx, cursor)
	}()

	loopFinished := false
	var loopResult error
	var shutdownCause error
	select {
	case <-rootCtx.Done():
		logger.Info("shutdown signal received")
	case err := <-healthErrors:
		shutdownCause = fmt.Errorf("health server failed: %w", err)
		stop()
	case err := <-loopErr:
		loopFinished = true
		loopResult = err
		stop()
	}

	ready.Store(false)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		logger.Warn("health server graceful shutdown failed", "error", err)
	}
	shutdownCancel()

	// Cancel the worker context and join the polling goroutine before closing
	// Kafka/MongoDB/OTel. Those dependencies must remain alive until Run exits.
	stop()
	if !loopFinished {
		waitTimer := time.NewTimer(shutdownWaitTimeout)
		select {
		case loopResult = <-loopErr:
			loopFinished = true
		case <-waitTimer.C:
			dependencyCleanupAllowed = false
			return fmt.Errorf("polling loop did not stop within %s; skipping dependency close to avoid closing resources while they are in use", shutdownWaitTimeout)
		}
		if !waitTimer.Stop() {
			select {
			case <-waitTimer.C:
			default:
			}
		}
	}
	if !loopFinished {
		dependencyCleanupAllowed = false
		return fmt.Errorf("polling loop stopped state could not be confirmed; skipping dependency cleanup")
	}
	if shutdownCause != nil {
		return shutdownCause
	}
	if loopResult != nil && !errors.Is(loopResult, context.Canceled) {
		return fmt.Errorf("polling loop stopped: %w", loopResult)
	}

	return nil
}

// Run performs an immediate poll, then continues on the configured interval.
// It preserves the last committed cursor when any publish/checkpoint operation fails.
func (a *CoastalAdapter) Run(ctx context.Context, initialCursor string) error {
	if a == nil {
		return fmt.Errorf("coastal adapter is nil")
	}

	cursor := initialCursor
	nextDelay := a.config.PollInterval

	for {
		freshnessCtx, freshnessCancel := context.WithTimeout(ctx, stateOperationTimeout)
		if err := a.state.RefreshDataStatus(freshnessCtx, time.Now().UTC(), a.policy.MaxObservationAge); err != nil && ctx.Err() == nil {
			a.logger.Warn("refreshing persisted source freshness failed", "error", err)
		}
		freshnessCancel()

		if err := a.retryUnknownStations(ctx); err != nil && ctx.Err() == nil {
			a.logger.Warn("retrying unknown-station observations failed; durable retry records retained", "error", err)
		}
		newCursor, advanced, err := a.pollOnce(ctx, cursor)
		if err != nil {
			if errors.Is(err, ErrCircuitOpen) {
				a.logger.Debug("Sensor Pantai circuit is open; skipping upstream request", "error", err)
				nextDelay = a.config.PollInterval
			} else {
				nextDelay = nextPollBackoff(nextDelay, a.config.PollInterval, 30*time.Second)
				a.logger.Warn("Sensor Pantai poll failed; retaining checkpoint", "error", err, "next_retry_in", nextDelay.String())
			}
		} else {
			if advanced {
				cursor = newCursor
			}
			nextDelay = a.config.PollInterval
		}

		timer := time.NewTimer(nextDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func nextPollBackoff(current, base, maximum time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if current < base {
		current = base
	}
	if current >= maximum/2 {
		return maximum
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

func (a *CoastalAdapter) pollOnce(ctx context.Context, cursor string) (string, bool, error) {
	if len(a.stations) == 0 {
		if err := a.refreshStations(ctx); err != nil {
			a.recordSourceFailure(ctx, err)
			return cursor, false, err
		}
	}

	pageCtx, pageSpan := a.telemetry.StartSpan(ctx, "sensor_pantai.fetch_observations")
	correlationID := a.telemetry.CorrelationID(pageCtx)
	pageSpan.SetAttributes(
		attribute.String("messaging.system", "sensor-pantai-http"),
		attribute.Bool("cursor.present", cursor != ""),
	)

	requestStarted := time.Now()
	page, err := a.sensor.FetchObservations(pageCtx, cursor)
	outcome := "success"
	if err != nil {
		outcome = upstreamOutcome(err)
		pageSpan.RecordError(err)
		pageSpan.SetStatus(codes.Error, "fetch observations failed")
	}
	a.telemetry.RecordUpstream(pageCtx, "observations", outcome, requestStarted)
	if err != nil {
		pageSpan.End()
		a.recordSourceFailure(ctx, err)
		return cursor, false, err
	}

	pageSpan.SetAttributes(attribute.Int("observations.count", len(page.Items)))
	pageHasFreshData := pageHasFreshObservation(page.Items, time.Now().UTC(), a.policy)
	// An empty page is a successful no-new-data poll. A non-empty page with no
	// fresh observation is not considered healthy, even when its JSON envelope
	// and HTTP status are valid.
	a.sensor.RecordObservationPageFreshness(pageHasFreshData || len(page.Items) == 0)
	if len(page.Items) > 0 && !pageHasFreshData {
		a.telemetry.RecordStalePage(pageCtx)
	}
	var lastDataAt *time.Time
	lastObservationID := ""
	refreshedStations := false

	for index, record := range page.Items {
		itemCtx, itemSpan := a.telemetry.StartSpan(pageCtx, "sensor_pantai.process_observation")
		itemSpan.SetAttributes(attribute.Int("observation.index", index))

		if record.DecodeErr != nil {
			err := a.publisher.PublishRawQuarantine(
				itemCtx,
				record.Raw,
				ReasonMalformedObservation,
				time.Now().UTC(),
				correlationID,
			)
			if err != nil {
				itemSpan.RecordError(err)
				itemSpan.SetStatus(codes.Error, "publish malformed item to quarantine failed")
				itemSpan.End()
				pageSpan.End()
				return cursor, false, fmt.Errorf("publish malformed observation item %d to quarantine: %w", index, err)
			}
			a.telemetry.RecordQuarantine(itemCtx, ReasonMalformedObservation)
			itemSpan.SetAttributes(attribute.String("observation.outcome", "quarantined"))
			itemSpan.End()
			continue
		}

		reading, reason := ValidateObservationRecord(record, a.stations, a.policy, time.Now().UTC())
		if reason == ReasonUnknownStation && !refreshedStations {
			refreshedStations = true
			if refreshErr := a.refreshStations(itemCtx); refreshErr != nil {
				a.logger.Warn("station metadata refresh failed", "correlation_id", correlationID, "error", refreshErr)
			}
			reading, reason = ValidateObservationRecord(record, a.stations, a.policy, time.Now().UTC())
		}

		if reason == ReasonUnknownStation {
			receivedAt := time.Now().UTC()
			retryRecord, enqueueErr := a.state.SaveRetryableObservation(itemCtx, record, receivedAt, correlationID)
			if enqueueErr != nil {
				itemSpan.RecordError(enqueueErr)
				itemSpan.SetStatus(codes.Error, "persist unknown-station retry failed")
				itemSpan.End()
				pageSpan.End()
				return cursor, false, fmt.Errorf("persist unknown-station observation %q: %w", observationID(record), enqueueErr)
			}
			if publishErr := a.publisher.PublishQuarantine(itemCtx, record, reason, receivedAt, correlationID); publishErr != nil {
				itemSpan.RecordError(publishErr)
				itemSpan.SetStatus(codes.Error, "publish retryable quarantine record failed")
				itemSpan.End()
				pageSpan.End()
				return cursor, false, fmt.Errorf("publish retryable unknown-station observation %q: %w", observationID(record), publishErr)
			}
			_ = retryRecord // returned record confirms idempotent durable enqueue
			a.telemetry.RecordRetryQueued(itemCtx)
			a.telemetry.RecordQuarantine(itemCtx, reason)
			itemSpan.SetAttributes(
				attribute.String("observation.outcome", "retry_queued"),
				attribute.String("observation.validation_reason", string(reason)),
			)
			itemSpan.End()
			continue
		}

		if reason != "" {
			err := a.publisher.PublishQuarantine(
				itemCtx,
				record,
				reason,
				time.Now().UTC(),
				correlationID,
			)
			if err != nil {
				itemSpan.RecordError(err)
				itemSpan.SetStatus(codes.Error, "publish invalid item to quarantine failed")
				itemSpan.End()
				pageSpan.End()
				return cursor, false, fmt.Errorf("publish invalid observation %q to quarantine: %w", observationID(record), err)
			}
			a.telemetry.RecordQuarantine(itemCtx, reason)
			itemSpan.SetAttributes(
				attribute.String("observation.outcome", "quarantined"),
				attribute.String("observation.validation_reason", string(reason)),
			)
			if record.Observation != nil && record.Observation.Critical != nil && *record.Observation.Critical {
				a.logger.Error(
					"critical observation failed validation and was quarantined; verify the critical-alert policy for invalid source data",
					"observation_id", record.Observation.ObservationID,
					"reason", string(reason),
					"correlation_id", correlationID,
				)
			}
			itemSpan.End()
			continue
		}

		event, err := NormalizeObservation(reading, time.Now().UTC())
		if err != nil {
			quarantineErr := a.publisher.PublishQuarantine(
				itemCtx,
				record,
				ReasonInvalidStationMetadata,
				time.Now().UTC(),
				correlationID,
			)
			if quarantineErr != nil {
				itemSpan.RecordError(quarantineErr)
				itemSpan.SetStatus(codes.Error, "publish normalization failure to quarantine failed")
				itemSpan.End()
				pageSpan.End()
				return cursor, false, fmt.Errorf("quarantine observation %q after normalization failure: %w", reading.ObservationID, quarantineErr)
			}
			a.telemetry.RecordQuarantine(itemCtx, ReasonInvalidStationMetadata)
			a.logger.Warn("observation normalization failed; item quarantined", "observation_id", reading.ObservationID, "error", err, "correlation_id", correlationID)
			itemSpan.End()
			continue
		}

		if err := a.publisher.PublishHazard(itemCtx, event, correlationID); err != nil {
			itemSpan.RecordError(err)
			itemSpan.SetStatus(codes.Error, "publish HazardEvent failed")
			itemSpan.End()
			pageSpan.End()
			return cursor, false, fmt.Errorf("publish HazardEvent %q: %w", event.HazardID, err)
		}

		a.telemetry.RecordHazardPublished(itemCtx)
		a.telemetry.RecordDataAge(itemCtx, reading.ObservedAt)
		itemSpan.SetAttributes(
			attribute.String("observation.outcome", "published"),
			attribute.String("hazard.id", event.HazardID),
			attribute.Bool("observation.critical", reading.Critical),
		)
		if lastDataAt == nil || reading.ObservedAt.After(*lastDataAt) {
			observedAt := reading.ObservedAt.UTC()
			lastDataAt = &observedAt
			lastObservationID = reading.ObservationID
		}
		itemSpan.End()
	}

	fetchedAt := time.Now().UTC()
	if page.NextCursor != "" && page.NextCursor != cursor {
		checkpointCtx, checkpointSpan := a.telemetry.StartSpan(pageCtx, "sensor_pantai.commit_checkpoint")
		if err := a.state.CommitPage(checkpointCtx, page.NextCursor, fetchedAt, lastDataAt, lastObservationID, a.policy.MaxObservationAge); err != nil {
			checkpointSpan.RecordError(err)
			checkpointSpan.SetStatus(codes.Error, "commit checkpoint failed")
			checkpointSpan.End()
			pageSpan.End()
			return cursor, false, fmt.Errorf("commit Sensor Pantai page checkpoint: %w", err)
		}
		a.telemetry.RecordCheckpoint(checkpointCtx)
		checkpointSpan.End()
		if lastDataAt != nil {
			a.telemetry.SetLastDataAt(*lastDataAt)
		}
		pageSpan.SetAttributes(attribute.Bool("checkpoint.advanced", true))
		pageSpan.End()
		return page.NextCursor, true, nil
	}

	// An empty page with no cursor advancement is a valid no-new-data poll.
	// If items were returned without a usable advancing cursor, do not commit:
	// the source contract must be verified before inventing a checkpoint value.
	if len(page.Items) > 0 {
		freshnessCtx, freshnessCancel := context.WithTimeout(ctx, stateOperationTimeout)
		if freshnessErr := a.state.RefreshDataStatus(freshnessCtx, fetchedAt, a.policy.MaxObservationAge); freshnessErr != nil {
			a.logger.Error("refresh data freshness after unusable cursor failed", "error", freshnessErr)
		}
		freshnessCancel()
		pageSpan.RecordError(fmt.Errorf("page contains items but next_cursor is empty or unchanged"))
		pageSpan.SetStatus(codes.Error, "cursor did not advance")
		pageSpan.End()
		return cursor, false, fmt.Errorf("Sensor Pantai returned %d item(s) but next_cursor is empty or unchanged; refusing to advance checkpoint", len(page.Items))
	}

	statusCtx, statusCancel := context.WithTimeout(ctx, stateOperationTimeout)
	statusErr := a.state.RecordSuccessfulFetch(statusCtx, fetchedAt, nil, "", a.policy.MaxObservationAge)
	statusCancel()
	if statusErr != nil {
		pageSpan.RecordError(statusErr)
		pageSpan.SetStatus(codes.Error, "record successful empty poll failed")
		pageSpan.End()
		return cursor, false, fmt.Errorf("record successful empty Sensor Pantai poll: %w", statusErr)
	}
	pageSpan.SetAttributes(attribute.Bool("checkpoint.advanced", false))
	pageSpan.End()
	return cursor, false, nil
}

// retryUnknownStations reprocesses durable observations whose station metadata
// was unknown at first receipt. It runs on every adapter cycle, but only loads
// records whose NextAttemptAt is due. Once metadata appears, the observation
// is normalized and published; the retry record is removed only after Kafka
// acknowledges both the HazardEvent and its recovered quarantine status.
func (a *CoastalAdapter) retryUnknownStations(ctx context.Context) error {
	breakerState, _, _ := a.sensor.BreakerState()
	if breakerState != CircuitClosed {
		// Let the next observations request own the HALF_OPEN probe. A retry
		// worker must not independently increase upstream traffic while OPEN.
		return nil
	}
	listCtx, listCancel := context.WithTimeout(ctx, stateOperationTimeout)
	pending, err := a.state.ListRetryableObservations(listCtx, time.Now().UTC(), retryableBatchLimit)
	listCancel()
	if err != nil {
		return fmt.Errorf("list retryable unknown-station observations: %w", err)
	}
	if len(pending) == 0 {
		return nil
	}

	retryCtx, retrySpan := a.telemetry.StartSpan(ctx, "sensor_pantai.retry_unknown_stations")
	defer retrySpan.End()
	if err := a.refreshStations(retryCtx); err != nil {
		for _, item := range pending {
			a.scheduleRetry(retryCtx, item, err)
		}
		return fmt.Errorf("refresh station metadata for retry queue: %w", err)
	}

	var firstErr error
	for _, item := range pending {
		itemCtx, itemSpan := a.telemetry.StartSpan(retryCtx, "sensor_pantai.retry_observation")
		itemSpan.SetAttributes(
			attribute.String("message.id", item.MessageID),
			attribute.String("observation.id", item.ObservationID),
			attribute.Int("retry.attempt", item.Attempts+1),
		)

		var observation Observation
		if err := json.Unmarshal(item.Raw, &observation); err != nil {
			if publishErr := a.publisher.PublishRawQuarantine(itemCtx, item.Raw, ReasonMalformedObservation, time.Now().UTC(), item.CorrelationID); publishErr != nil {
				a.scheduleRetry(itemCtx, item, publishErr)
				itemSpan.RecordError(publishErr)
				itemSpan.End()
				if firstErr == nil {
					firstErr = publishErr
				}
				continue
			}
			if statusErr := a.publisher.PublishRetryStatus(itemCtx, item, "parked", string(ReasonMalformedObservation)); statusErr != nil {
				a.scheduleRetry(itemCtx, item, statusErr)
				itemSpan.RecordError(statusErr)
				itemSpan.End()
				if firstErr == nil {
					firstErr = statusErr
				}
				continue
			}
			if deleteErr := a.state.DeleteRetryableObservation(itemCtx, item.MessageID); deleteErr != nil {
				a.scheduleRetry(itemCtx, item, deleteErr)
				itemSpan.RecordError(deleteErr)
				itemSpan.End()
				if firstErr == nil {
					firstErr = deleteErr
				}
				continue
			}
			a.telemetry.RecordQuarantine(itemCtx, ReasonMalformedObservation)
			itemSpan.SetAttributes(attribute.String("retry.outcome", "parked_malformed"))
			itemSpan.End()
			continue
		}

		record := ObservationRecord{Raw: append([]byte(nil), item.Raw...), Observation: &observation}
		reading, reason := ValidateObservationRecord(record, a.stations, a.policy, time.Now().UTC())
		if reason == ReasonUnknownStation {
			a.scheduleRetry(itemCtx, item, fmt.Errorf("station %q is still unknown", observation.StationID))
			itemSpan.SetAttributes(attribute.String("retry.outcome", "station_still_unknown"))
			itemSpan.End()
			continue
		}
		if reason != "" {
			if err := a.publisher.PublishQuarantine(itemCtx, record, reason, time.Now().UTC(), item.CorrelationID); err != nil {
				a.scheduleRetry(itemCtx, item, err)
				itemSpan.RecordError(err)
				itemSpan.End()
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if err := a.publisher.PublishRetryStatus(itemCtx, item, "parked", string(reason)); err != nil {
				a.scheduleRetry(itemCtx, item, err)
				itemSpan.RecordError(err)
				itemSpan.End()
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if err := a.state.DeleteRetryableObservation(itemCtx, item.MessageID); err != nil {
				a.scheduleRetry(itemCtx, item, err)
				itemSpan.RecordError(err)
				itemSpan.End()
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			a.telemetry.RecordQuarantine(itemCtx, reason)
			itemSpan.SetAttributes(attribute.String("retry.outcome", "parked_after_revalidation"))
			itemSpan.End()
			continue
		}

		event, err := NormalizeObservation(reading, time.Now().UTC())
		if err == nil {
			err = a.publisher.PublishHazard(itemCtx, event, item.CorrelationID)
		}
		if err == nil {
			err = a.publisher.PublishRetryStatus(itemCtx, item, "recovered", "")
		}
		if err == nil {
			err = a.state.RecordLastGoodObservation(itemCtx, reading.ObservedAt, reading.ObservationID, time.Now().UTC(), a.policy.MaxObservationAge)
		}
		if err == nil {
			err = a.state.DeleteRetryableObservation(itemCtx, item.MessageID)
		}
		if err != nil {
			a.scheduleRetry(itemCtx, item, err)
			itemSpan.RecordError(err)
			itemSpan.SetStatus(codes.Error, "retryable observation recovery failed")
			itemSpan.End()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		a.telemetry.RecordHazardPublished(itemCtx)
		a.telemetry.RecordRetryRecovered(itemCtx)
		a.telemetry.RecordDataAge(itemCtx, reading.ObservedAt)
		itemSpan.SetAttributes(
			attribute.String("retry.outcome", "recovered"),
			attribute.String("hazard.id", event.HazardID),
		)
		itemSpan.End()
	}
	return firstErr
}

func (a *CoastalAdapter) scheduleRetry(ctx context.Context, item RetryableObservation, cause error) {
	attempt := item.Attempts + 1
	delay := retryBackoff(attempt)
	now := time.Now().UTC()
	message := "retry failed"
	if cause != nil {
		message = cause.Error()
	}
	if err := a.state.MarkRetryAttempt(ctx, item.MessageID, now, now.Add(delay), message); err != nil {
		a.logger.Error("failed to reschedule retryable observation", "message_id", item.MessageID, "cause", cause, "state_error", err)
	}
	a.telemetry.RecordRetryAttempt(ctx)
}

func retryBackoff(attempt int) time.Duration {
	delay := initialRetryBackoff
	if attempt < 0 {
		attempt = 0
	}
	for i := 0; i < attempt && delay < maximumRetryBackoff; i++ {
		delay *= 2
	}
	if delay > maximumRetryBackoff {
		return maximumRetryBackoff
	}
	return delay
}

// pageHasFreshObservation checks event timestamps before the breaker records
// a successful page. Unknown-station observations can still prove that the
// source is delivering fresh data, even though they are not yet last-known-good.
func pageHasFreshObservation(records []ObservationRecord, now time.Time, policy ValidationPolicy) bool {
	for _, record := range records {
		if record.Observation == nil || record.Observation.ObservedAt == nil || record.Observation.ObservedAt.IsZero() {
			continue
		}
		age := now.Sub(record.Observation.ObservedAt.UTC())
		if age <= policy.MaxObservationAge && age >= -policy.MaxFutureSkew {
			return true
		}
	}
	return false
}

func (a *CoastalAdapter) refreshStations(ctx context.Context) error {
	spanCtx, span := a.telemetry.StartSpan(ctx, "sensor_pantai.fetch_stations")
	defer span.End()

	startedAt := time.Now()
	stations, err := a.sensor.FetchStations(spanCtx)
	outcome := "success"
	if err != nil {
		outcome = upstreamOutcome(err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "fetch stations failed")
	}
	a.telemetry.RecordUpstream(spanCtx, "stations", outcome, startedAt)
	if err != nil {
		return err
	}
	if len(stations) == 0 {
		return fmt.Errorf("Sensor Pantai returned an empty station list")
	}

	stationMap := make(map[string]Station, len(stations))
	for _, station := range stations {
		stationID := strings.TrimSpace(station.StationID)
		if stationID == "" || validateStationMetadata(stationID, station) != "" {
			a.logger.Warn("ignoring invalid station metadata", "station_id", stationID)
			continue
		}
		stationMap[stationID] = station
	}
	if len(stationMap) == 0 {
		return fmt.Errorf("Sensor Pantai returned no valid station metadata")
	}
	a.stations = stationMap
	span.SetAttributes(attribute.Int("stations.count", len(stationMap)))
	a.logger.Info("Sensor Pantai station metadata loaded", "count", len(stationMap))
	return nil
}

func (a *CoastalAdapter) recordSourceFailure(ctx context.Context, cause error) {
	if errors.Is(cause, ErrCircuitOpen) || ctx.Err() != nil {
		return
	}
	failureCtx, cancel := context.WithTimeout(ctx, stateOperationTimeout)
	defer cancel()
	if err := a.state.RecordSourceFailure(failureCtx, cause, time.Now().UTC(), a.policy.MaxObservationAge); err != nil {
		a.logger.Error("failed to persist Sensor Pantai source failure", "cause", cause, "state_error", err)
	}
}

func upstreamOutcome(err error) string {
	if errors.Is(err, ErrCircuitOpen) {
		return "circuit_open"
	}
	return "error"
}

func observationID(record ObservationRecord) string {
	if record.Observation == nil {
		return ""
	}
	return record.Observation.ObservationID
}

func loadValidationPolicyFromEnv() (ValidationPolicy, error) {
	maxAgeRaw, ok := lookupEnv(validationMaxAgeEnv)
	if !ok || strings.TrimSpace(maxAgeRaw) == "" {
		return ValidationPolicy{}, fmt.Errorf("%s is required; choose it from the observed Sensor Pantai freshness/cadence and document the policy", validationMaxAgeEnv)
	}
	maxAgeMS, err := strconv.ParseInt(strings.TrimSpace(maxAgeRaw), 10, 64)
	if err != nil || maxAgeMS <= 0 || maxAgeMS > maxDurationMilliseconds {
		return ValidationPolicy{}, fmt.Errorf("%s must be a positive, representable duration in milliseconds", validationMaxAgeEnv)
	}

	futureSkewMS := int64(0)
	if raw, exists := lookupEnv(validationFutureEnv); exists && strings.TrimSpace(raw) != "" {
		futureSkewMS, err = strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil || futureSkewMS < 0 || futureSkewMS > maxDurationMilliseconds {
			return ValidationPolicy{}, fmt.Errorf("%s must be a non-negative, representable duration in milliseconds", validationFutureEnv)
		}
	}

	policy := ValidationPolicy{
		MaxObservationAge: time.Duration(maxAgeMS) * time.Millisecond,
		MaxFutureSkew:     time.Duration(futureSkewMS) * time.Millisecond,
	}
	if err := policy.Validate(); err != nil {
		return ValidationPolicy{}, err
	}
	return policy, nil
}

func newHealthServer(port int, ready *atomic.Bool, sensor *SensorClient) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeHealthJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready == nil || !ready.Load() {
			writeHealthJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready"})
			return
		}
		state, failures, openUntil := sensor.BreakerState()
		response := map[string]any{
			"status":               "ready",
			"sensor_circuit_state": state.String(),
			"sensor_failures":      failures,
		}
		if !openUntil.IsZero() {
			response["sensor_probe_after"] = openUntil.UTC().Format(time.RFC3339Nano)
		}
		writeHealthJSON(w, http.StatusOK, response)
	})

	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
}

func writeHealthJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		// The status is already written; nothing useful can be sent to the client.
		return
	}
}
