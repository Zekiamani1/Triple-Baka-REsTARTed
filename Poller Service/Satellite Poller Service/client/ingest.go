// Package ingestion receives Satellite Feed events and durably stores them
// before the processing worker handles them.
//
// This file intentionally owns only feed ingestion and reconnect behavior.
// Detection processing, clustering, and Kafka publication belong to their
// respective modules.
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"satellitepoller/domain"
	"strconv"
	"strings"
	"time"
)

const feedCheckpointName = "satellite-feed"

var (
	// kalau feed mengirim offset melewati expected offset
	ErrOffsetGap = errors.New("satellite feed offset gap detected")

	// self-explanatory, kalau offset negatif
	ErrInvalidOffset = errors.New("invalid satellite feed offset")
)

// Repository must persist an inbox event and its feed checkpoint atomically.
// If the same feed offset is delivered again, the implementation should treat
// it as an idempotent duplicate rather than inserting a second inbox record.
type Repository interface {
	GetCheckpoint(ctx context.Context, name string) (domain.Checkpoint, bool, error)
	SaveInboxEventAndCheckpoint(
		ctx context.Context,
		event domain.InboxEvent,
		checkpoint domain.Checkpoint,
	) error
}

type Options struct {
	// TL;DR just in case mulainya gak di 0
	InitialOffset string

	// base delay kalau stream disconnect
	ReconnectDelay  time.Duration
	LatestOffset    int64
	HasLatestOffset bool
	Logger          *slog.Logger
}

type Service struct {
	client Client
	repo   Repository
	opts   Options
	logger *slog.Logger
}

// NewService creates an ingestion service.
func NewService(client *Client, repo Repository, opts Options) (*Service, error) {
	if client == nil {
		return nil, errors.New("feed client is required")
	}
	if repo == nil {
		return nil, errors.New("ingestion repository is required")
	}
	if opts.InitialOffset == "" {
		opts.InitialOffset = "0"
	}
	if _, err := parseOffset(opts.InitialOffset); err != nil {
		return nil, fmt.Errorf("invalid initial offset: %w", err)
	}
	if opts.ReconnectDelay <= 0 {
		opts.ReconnectDelay = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.HasLatestOffset && opts.LatestOffset < 0 {
		return nil, errors.New("session latest offset cannot be negative")
	}

	return &Service{
		client: *client,
		repo:   repo,
		opts:   opts,
		logger: opts.Logger.With("component", "satellite-ingestion"),
	}, nil
}

// makan feed sampai ctx cancel / gak bisa retry
func (s *Service) Run(ctx context.Context) error {
	checkpoint, found, err := s.repo.GetCheckpoint(ctx, feedCheckpointName)
	if err != nil {
		return fmt.Errorf("load feed checkpoint: %w", err)
	}

	lastOffset, hasCheckpoint := int64(0), found
	from := s.opts.InitialOffset
	if found {
		lastOffset = checkpoint.Offset
		from = strconv.FormatInt(lastOffset, 10)
	}

	if s.opts.HasLatestOffset {
		if found && checkpoint.Offset > s.opts.LatestOffset {
			return fmt.Errorf(
				"stored feed checkpoint %d is ahead of session latest_offset %d",
				checkpoint.Offset,
				s.opts.LatestOffset,
			)
		}

		if !found {
			initial, err := parseOffset(s.opts.InitialOffset)
			if err != nil {
				return fmt.Errorf("parse initial offset: %w", err)
			}

			if initial > s.opts.LatestOffset {
				return fmt.Errorf(
					"initial offset %d is ahead of session latest_offset %d",
					initial,
					s.opts.LatestOffset,
				)
			}
		}
	}

	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		resp, err := s.client.OpenStream(ctx, from)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if isPermanentFeedError(err) {
				return fmt.Errorf("open satellite stream: %w", err)
			}

			delay := s.nextDelay(attempt, err)
			attempt++
			s.logger.Warn("satellite stream connection failed; retrying",
				"from_offset", from,
				"retry_in", delay,
				"error", err,
			)
			if err := wait(ctx, delay); err != nil {
				return nil
			}
			continue
		}

		s.logger.Info("satellite stream connected", "from_offset", from)
		processedOnConnection := false

		readErr := s.client.ReadEvents(ctx, resp.Body, func(event Event) error {
			offset, err := parseOffset(event.Offset)
			if err != nil {
				return err
			}

			if hasCheckpoint && offset <= lastOffset {
				return nil
			}

			if hasCheckpoint && offset != lastOffset+1 {
				return fmt.Errorf("%w: last=%d received=%d", ErrOffsetGap, lastOffset, offset)
			}

			now := time.Now().UTC()
			inboxEvent := domain.InboxEvent{
				Offset:     offset,
				Type:       domain.EventType(event.Type),
				RawPayload: append([]byte(nil), event.Data...),
				ReceivedAt: now,
			}
			newCheckpoint := domain.Checkpoint{
				Name:      feedCheckpointName,
				Offset:    offset,
				UpdatedAt: now,
			}

			if err := s.repo.SaveInboxEventAndCheckpoint(ctx, inboxEvent, newCheckpoint); err != nil {
				return fmt.Errorf("persist feed event at offset %d: %w", offset, err)
			}

			lastOffset = offset
			hasCheckpoint = true
			from = strconv.FormatInt(lastOffset, 10)
			processedOnConnection = true
			s.logger.Debug("satellite event persisted",
				"offset", offset,
				"event_type", event.Type,
			)
			return nil
		})

		_ = resp.Body.Close()

		if ctx.Err() != nil {
			return nil
		}

		if errors.Is(readErr, ErrOffsetGap) || errors.Is(readErr, ErrInvalidOffset) {
			return fmt.Errorf("satellite stream contract violation: %w", readErr)
		}

		if processedOnConnection {
			attempt = 0
		}

		delay := s.nextDelay(attempt, readErr)
		attempt++
		s.logger.Warn("satellite stream ended; reconnecting",
			"last_durable_offset", lastOffset,
			"retry_in", delay,
			"error", readErr,
		)
		if err := wait(ctx, delay); err != nil {
			return nil
		}
	}
}

func parseOffset(value string) (int64, error) {
	value = strings.TrimSpace(value)
	offset, err := strconv.ParseInt(value, 10, 64)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("%w: %q", ErrInvalidOffset, value)
	}
	return offset, nil
}

func (s *Service) nextDelay(attempt int, cause error) time.Duration {
	if httpErr := new(HTTPError); errors.As(cause, &httpErr) && httpErr.RetryAfter != "" {
		if delay, ok := parseRetryAfter(httpErr.RetryAfter); ok {
			return delay
		}
	}

	delay := s.opts.ReconnectDelay
	for i := 0; i < attempt && delay < time.Minute; i++ {
		delay *= 2
	}
	if delay > time.Minute {
		delay = time.Minute
	}
	return delay
}

func parseRetryAfter(value string) (time.Duration, bool) {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}

	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := time.Until(when)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}

func isPermanentFeedError(err error) bool {
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		return false // Network and transport errors are retryable.
	}

	switch httpErr.StatusCode {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return true
	default:
		return false // Includes 429 and 5xx responses.
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
