package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	// maxSensorResponseBytes bounds memory use when reading a response from the
	// external sensor. Increase only after measuring the actual response size.
	maxSensorResponseBytes int64 = 8 << 20 // 8 MiB

	healthPath        = "/healthz"
	stationsPath      = "/v1/stations"
	observationsPath  = "/v1/observations"
	defaultAcceptType = "application/json"
)

var ErrCircuitOpen = errors.New("coastal sensor circuit breaker is open")

// CircuitState represents the state of the sensor circuit breaker.
type CircuitState uint8

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// CircuitOpenError provides the time at which a new probe may be attempted.
// It also supports errors.Is(err, ErrCircuitOpen).
type CircuitOpenError struct {
	RetryAt time.Time
}

func (e *CircuitOpenError) Error() string {
	if e == nil || e.RetryAt.IsZero() {
		return ErrCircuitOpen.Error()
	}
	return fmt.Sprintf("%s; next probe may start at %s", ErrCircuitOpen, e.RetryAt.UTC().Format(time.RFC3339Nano))
}

func (e *CircuitOpenError) Is(target error) bool {
	return target == ErrCircuitOpen
}

// CircuitBreaker protects the adapter from repeatedly calling an unhealthy
// Sensor Pantai endpoint. In HALF_OPEN, exactly one caller is allowed to probe.
type CircuitBreaker struct {
	mu                sync.Mutex
	state             CircuitState
	consecutiveErrors int
	failureThreshold  int
	openDuration      time.Duration
	openUntil         time.Time
}

// NewCircuitBreaker creates a breaker with validated configuration values.
func NewCircuitBreaker(failureThreshold int, openDuration time.Duration) (*CircuitBreaker, error) {
	if failureThreshold <= 0 {
		return nil, fmt.Errorf("breaker failure threshold must be greater than zero")
	}
	if openDuration <= 0 {
		return nil, fmt.Errorf("breaker open duration must be greater than zero")
	}

	return &CircuitBreaker{
		state:            CircuitClosed,
		failureThreshold: failureThreshold,
		openDuration:     openDuration,
	}, nil
}

// Do executes fn only when the breaker allows a request. An error returned by
// fn counts as an upstream failure; item-level decode errors are intentionally
// handled after the response envelope has been fetched and do not open breaker.
func (b *CircuitBreaker) Do(fn func() error) error {
	if fn == nil {
		return fmt.Errorf("circuit breaker operation must not be nil")
	}

	if err := b.allow(); err != nil {
		return err
	}

	err := fn()
	if err != nil {
		b.recordFailure()
		return err
	}

	b.recordSuccess()
	return nil
}

func (b *CircuitBreaker) allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	switch b.state {
	case CircuitClosed:
		return nil
	case CircuitOpen:
		if now.Before(b.openUntil) {
			return &CircuitOpenError{RetryAt: b.openUntil}
		}
		// The first caller after the cooldown becomes the single HALF_OPEN
		// probe. Concurrent callers are rejected until it completes.
		b.state = CircuitHalfOpen
		return nil
	case CircuitHalfOpen:
		return &CircuitOpenError{}
	default:
		return fmt.Errorf("invalid circuit breaker state: %d", b.state)
	}
}

func (b *CircuitBreaker) recordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.state = CircuitClosed
	b.consecutiveErrors = 0
	b.openUntil = time.Time{}
}

func (b *CircuitBreaker) recordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == CircuitHalfOpen {
		b.open()
		return
	}

	b.consecutiveErrors++
	if b.consecutiveErrors >= b.failureThreshold {
		b.open()
	}
}

func (b *CircuitBreaker) open() {
	b.state = CircuitOpen
	b.openUntil = time.Now().Add(b.openDuration)
}

// State returns a thread-safe snapshot of the current breaker state.
func (b *CircuitBreaker) State() (CircuitState, int, time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state, b.consecutiveErrors, b.openUntil
}

// Station describes one station from GET /v1/stations. Pointer coordinates
// allow validation to distinguish a missing coordinate from the valid value 0.
type Station struct {
	StationID string   `json:"station_id"`
	Name      string   `json:"name"`
	Province  string   `json:"province"`
	Latitude  *float64 `json:"latitude"`
	Longitude *float64 `json:"longitude"`
}

type stationsEnvelope struct {
	Items *[]Station `json:"items"`
}

// Observation is the typed form of one sensor observation. Pointer fields
// preserve the difference between a missing field and its zero value.
type Observation struct {
	ObservationID    string     `json:"observation_id"`
	StationID        string     `json:"station_id"`
	ObservedAt       *time.Time `json:"observed_at"`
	SeaLevelAnomalyM *float64   `json:"sea_level_anomaly_m"`
	WaveHeightM      *float64   `json:"wave_height_m"`
	Quality          *string    `json:"quality"`
	Critical         *bool      `json:"critical"`
}

type observationsEnvelope struct {
	GeneratedAt *time.Time         `json:"generated_at"`
	Items       *[]json.RawMessage `json:"items"`
	NextCursor  *string            `json:"next_cursor"`
}

// ObservationRecord keeps the original payload even when its individual item
// cannot be decoded. The caller can quarantine Raw with DecodeErr while still
// processing other records from the same valid envelope.
type ObservationRecord struct {
	Raw         json.RawMessage
	Observation *Observation
	DecodeErr   error
}

// ObservationPage is a decoded page returned by GET /v1/observations.
type ObservationPage struct {
	GeneratedAt *time.Time
	Items       []ObservationRecord
	NextCursor  string
}

// HTTPStatusError represents a non-2xx response from Sensor Pantai.
type HTTPStatusError struct {
	Method     string
	URL        string
	StatusCode int
	Body       string
}

func (e *HTTPStatusError) Error() string {
	if e == nil {
		return "sensor returned an HTTP error"
	}
	if e.Body == "" {
		return fmt.Sprintf("sensor request %s %s returned HTTP %d", e.Method, e.URL, e.StatusCode)
	}
	return fmt.Sprintf("sensor request %s %s returned HTTP %d: %s", e.Method, e.URL, e.StatusCode, e.Body)
}

// SensorClient is the HTTP client for the Sensor Pantai contract.
type SensorClient struct {
	baseURL          string
	authToken        string
	httpClient       *http.Client
	breaker          *CircuitBreaker
	maxResponseBytes int64
}

// NewSensorClient creates the HTTP client and its circuit breaker from config.go.
func NewSensorClient(cfg Config) (*SensorClient, error) {
	if strings.TrimSpace(cfg.SensorBaseURL) == "" {
		return nil, fmt.Errorf("sensor base URL must not be empty")
	}
	if strings.TrimSpace(cfg.PantaiAuthToken) == "" {
		return nil, fmt.Errorf("Sensor Pantai auth token must not be empty")
	}
	if cfg.HTTPTimeout <= 0 {
		return nil, fmt.Errorf("sensor HTTP timeout must be greater than zero")
	}

	breaker, err := NewCircuitBreaker(cfg.BreakerFailureThreshold, cfg.BreakerOpenDuration)
	if err != nil {
		return nil, err
	}

	return &SensorClient{
		baseURL:   strings.TrimRight(strings.TrimSpace(cfg.SensorBaseURL), "/"),
		authToken: strings.TrimSpace(cfg.PantaiAuthToken),
		httpClient: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
		breaker:          breaker,
		maxResponseBytes: maxSensorResponseBytes,
	}, nil
}

// FetchStations retrieves station metadata through the authenticated endpoint.
// It participates in the breaker, but observation freshness is evaluated only
// after FetchObservations returns and the caller inspects item timestamps.
func (c *SensorClient) FetchStations(ctx context.Context) ([]Station, error) {
	if c == nil {
		return nil, fmt.Errorf("sensor client is nil")
	}

	var stations []Station
	err := c.breaker.Do(func() error {
		var envelope stationsEnvelope
		if err := c.getJSON(ctx, stationsPath, nil, true, &envelope); err != nil {
			return err
		}
		if envelope.Items == nil {
			return fmt.Errorf("invalid stations response: missing or null items")
		}
		stations = *envelope.Items
		return nil
	})
	if err != nil {
		return nil, err
	}
	return stations, nil
}

// FetchObservations retrieves one cursor-based page. The cursor is opaque and
// is passed back to the sensor unchanged; an empty cursor is omitted so the
// source applies its initial-page behavior. It checks breaker permission and
// records transport/envelope failures, but leaves success/freshness completion
// to RecordObservationPageFreshness after item timestamps have been inspected.
//
// Individual items are decoded independently. A malformed item is returned in
// ObservationRecord with its original Raw bytes and DecodeErr, while valid
// items in the same page remain available to the caller.
func (c *SensorClient) FetchObservations(ctx context.Context, cursor string) (ObservationPage, error) {
	if c == nil {
		return ObservationPage{}, fmt.Errorf("sensor client is nil")
	}
	if err := c.breaker.allow(); err != nil {
		return ObservationPage{}, err
	}

	query := url.Values{}
	if cursor != "" {
		query.Set("cursor", cursor)
	}

	var envelope observationsEnvelope
	if err := c.getJSON(ctx, observationsPath, query, true, &envelope); err != nil {
		if ctx.Err() == nil {
			c.breaker.recordFailure()
		}
		return ObservationPage{}, err
	}
	if envelope.Items == nil {
		err := fmt.Errorf("invalid observations response: missing or null items")
		c.breaker.recordFailure()
		return ObservationPage{}, err
	}

	page := ObservationPage{GeneratedAt: envelope.GeneratedAt}
	if envelope.NextCursor != nil {
		page.NextCursor = *envelope.NextCursor
	}
	page.Items = make([]ObservationRecord, 0, len(*envelope.Items))
	for i, raw := range *envelope.Items {
		record := ObservationRecord{Raw: append(json.RawMessage(nil), raw...)}
		var observation Observation
		if err := json.Unmarshal(raw, &observation); err != nil {
			record.DecodeErr = fmt.Errorf("decode observation item %d: %w", i, err)
		} else {
			record.Observation = &observation
		}
		page.Items = append(page.Items, record)
	}

	// Do not close the breaker here. A syntactically valid envelope may contain
	// only stale observations. The caller reports whether this page contains
	// fresh sensor data after applying the configured freshness policy.
	return page, nil
}

// RecordObservationPageFreshness completes the breaker decision after the
// caller has inspected event-time freshness. A stale-only or malformed page
// counts as an upstream data-quality failure; an empty page is considered a
// successful poll by the caller because no stale item was returned.
func (c *SensorClient) RecordObservationPageFreshness(hasFreshData bool) {
	if c == nil || c.breaker == nil {
		return
	}
	if hasFreshData {
		c.breaker.recordSuccess()
	} else {
		c.breaker.recordFailure()
	}
}

// HealthCheck calls /healthz without authentication. This only verifies that
// the sensor process responds; it does not prove that observation data is fresh
// or valid and must not by itself close the circuit breaker.
func (c *SensorClient) HealthCheck(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("sensor client is nil")
	}

	requestURL := c.baseURL + healthPath
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return fmt.Errorf("create sensor health request: %w", err)
	}
	request.Header.Set("Accept", defaultAcceptType)

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("sensor health request failed: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := readLimitedBody(response.Body, 4096)
		return &HTTPStatusError{
			Method:     http.MethodGet,
			URL:        requestURL,
			StatusCode: response.StatusCode,
			Body:       strings.TrimSpace(string(body)),
		}
	}

	// Consume a bounded amount so the connection can be reused safely. The
	// health response is tiny; oversized bodies are treated as malformed.
	body, err := readLimitedBody(response.Body, 4096)
	if err != nil {
		return fmt.Errorf("read sensor health response: %w", err)
	}
	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("decode sensor health response: %w", err)
	}
	if result.Status != "ok" {
		return fmt.Errorf("sensor health response has unexpected status %q", result.Status)
	}
	return nil
}

// BreakerState exposes the current breaker state for telemetry and health
// reporting. It does not trigger a probe.
func (c *SensorClient) BreakerState() (CircuitState, int, time.Time) {
	if c == nil || c.breaker == nil {
		return CircuitState(255), 0, time.Time{}
	}
	return c.breaker.State()
}

func (c *SensorClient) getJSON(
	ctx context.Context,
	path string,
	query url.Values,
	authenticated bool,
	out any,
) error {
	requestURL, err := url.Parse(c.baseURL + path)
	if err != nil {
		return fmt.Errorf("build sensor request URL: %w", err)
	}
	if len(query) > 0 {
		requestURL.RawQuery = query.Encode()
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return fmt.Errorf("create sensor request: %w", err)
	}
	request.Header.Set("Accept", defaultAcceptType)
	if authenticated {
		request.Header.Set("Authorization", "Bearer "+c.authToken)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("sensor request %s failed: %w", path, err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, readErr := readLimitedBody(response.Body, 4096)
		if readErr != nil {
			return fmt.Errorf("sensor request %s returned HTTP %d; read error body: %w", path, response.StatusCode, readErr)
		}
		return &HTTPStatusError{
			Method:     http.MethodGet,
			URL:        requestURL.String(),
			StatusCode: response.StatusCode,
			Body:       strings.TrimSpace(string(body)),
		}
	}

	body, err := readLimitedBody(response.Body, c.maxResponseBytes)
	if err != nil {
		return fmt.Errorf("read sensor response %s: %w", path, err)
	}
	if len(body) == 0 {
		return fmt.Errorf("sensor response %s is empty", path)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("decode sensor response %s: %w", path, err)
	}
	return nil
}

func readLimitedBody(body io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("maximum response size must be greater than zero")
	}

	limitedReader := io.LimitReader(body, maxBytes+1)
	data, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("response exceeds maximum allowed size of %d bytes", maxBytes)
	}
	return data, nil
}
