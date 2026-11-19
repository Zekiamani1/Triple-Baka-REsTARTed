package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Client struct {
	baseURL      *url.URL
	groupToken   string
	httpClient   *http.Client
	maxLineSize  int
	maxEventSize int
}

type Session map[string]json.RawMessage

type HTTPError struct {
	StatusCode int
	Body       string
	RetryAfter string
}

type Event struct {
	Offset string          `json:"offset"`
	Type   string          `json:"type"`
	Data   json.RawMessage `json:"data"`
}

func NewClient(baseURL, groupToken string, httpClient *http.Client) (*Client, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("baseURL cannot be empty")
	}
	parsedUrl, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse baseURL: %w", err)
	}
	if parsedUrl.Scheme != "http" && parsedUrl.Scheme != "https" {
		return nil, fmt.Errorf("baseURL must have http or https scheme %q", parsedUrl)
	}
	if parsedUrl.Host == "" {
		return nil, fmt.Errorf("baseURL must have a host %q", parsedUrl)
	}
	if strings.TrimSpace(groupToken) == "" {
		return nil, fmt.Errorf("groupToken cannot be empty")
	}
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &Client{
		baseURL:      parsedUrl,
		groupToken:   groupToken,
		httpClient:   httpClient,
		maxLineSize:  1 << 20,
		maxEventSize: 4 << 20,
	}, nil
}

// GetSession calls GET /v1/session and returns the JSON fields without assuming
// types for profile/speed/virtual_now/latest_offset. Callers can decode the
// individual fields after confirming the feed's actual schema.
func (c *Client) GetSession(ctx context.Context) (Session, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/v1/session", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, c.newHTTPError(resp)
	}
	var sesh Session
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&sesh); err != nil {
		return nil, fmt.Errorf("failed to decode response body: %w", err)
	}
	if sesh == nil {
		return nil, errors.New("response body is not a valid JSON object")
	}
	return sesh, nil
}

// OpenStream calls GET /v1/stream?from=<offset> and returns the open response
// body. The caller must close resp.Body. A successful response should have
// Content-Type text/event-stream.
//
// The exact inclusive/exclusive meaning of "from" must be confirmed against
// the feed contract/behavior; this method intentionally does not adjust it.
func (c *Client) OpenStream(ctx context.Context, from string) (*http.Response, error) {
	if strings.TrimSpace(from) == "" {
		return nil, errors.New("stream starting offset is required")
	}
	req, err := c.newRequest(ctx, http.MethodGet, "/v1/stream", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	query := req.URL.Query()
	query.Set("from", from)
	req.URL.RawQuery = query.Encode()
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}

		return nil, fmt.Errorf("failed to perform request: %w", err)
	}
	if resp == nil {
		return nil, errors.New("received nil response from HTTP client")
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, c.newHTTPError(resp)
	}

	mediaType, _, err := mime.ParseMediaType(
		resp.Header.Get("Content-Type"),
	)

	if err != nil ||
		!strings.EqualFold(mediaType, "text/event-stream") {

		_ = resp.Body.Close()

		return nil, fmt.Errorf(
			"unexpected stream content-type: %q",
			resp.Header.Get("Content-Type"),
		)
	}
	return resp, nil
}

// ReadEvents parses an SSE response body and invokes handle for each complete
// event. It returns when the stream reaches EOF, the context is cancelled, the
// stream is malformed/too large, or handle returns an error.
//
// A complete SSE event ends with a blank line. An incomplete event at EOF is
// discarded so a truncated payload is never passed to the processor.
func (c *Client) ReadEvents(ctx context.Context, body io.Reader, handle func(Event) error) error {
	if body == nil {
		return errors.New("body cannot be nil")
	}
	if handle == nil {
		return errors.New("SSE event handler cannot be nil")
	}

	scanner := bufio.NewScanner(body)
	initBuf := make([]byte, 0, 64*1024)
	scanner.Buffer(initBuf, c.maxLineSize)
	var eventID string
	var eventType string
	var data bytes.Buffer

	resetEvent := func() {
		eventID = ""
		eventType = ""
		data.Reset()
	}

	dispatch := func() error {
		if data.Len() == 0 {
			resetEvent()
			return nil
		}
		if eventID == "" {
			return errors.New("SSE event is missing its id/offset")
		}
		if eventType == "" {
			return errors.New("SSE event is missing its event type")
		}

		payload := bytes.TrimSuffix(data.Bytes(), []byte("\n"))
		event := Event{
			Offset: eventID,
			Type:   eventType,
			Data:   append(json.RawMessage(nil), payload...),
		}
		if !json.Valid(event.Data) {
			return fmt.Errorf("SSE event at offset %q contains invalid JSON", event.Offset)
		}
		if err := handle(event); err != nil {
			return fmt.Errorf("handle SSE event at offset %q (%s): %w", event.Offset, event.Type, err)
		}
		resetEvent()
		return nil
	}

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Text()

		// A blank line terminates one SSE event.
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}

		// Comments
		if strings.HasPrefix(line, ":") {
			continue
		}

		field, value, found := strings.Cut(line, ":")
		if !found {
			value = ""
		}
		// The SSE spec removes at most one leading space after the colon.
		value = strings.TrimPrefix(value, " ")

		switch field {
		case "id":
			// Per SSE, an id containing NUL is ignored.
			if !strings.ContainsRune(value, '\x00') {
				eventID = value
			}
		case "event":
			eventType = value
		case "data":
			data.WriteString(value)
			data.WriteByte('\n')
			if data.Len() > c.maxEventSize {
				return fmt.Errorf("SSE event exceeds maximum size of %d bytes", c.maxEventSize)
			}
		case "retry":
			// Reconnect policy belongs to ingestion.Service. It can be extended
			// to honor this field if the feed contract requires it.
		default:
			// Unknown SSE fields are ignored by the SSE format.
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read satellite SSE stream: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Do not dispatch a non-terminated event: it may be a partial frame caused
	// by a broken connection. Reconnect from the last durable checkpoint.
	return io.EOF
}

func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	url := *c.baseURL
	url.Path = strings.TrimRight(c.baseURL.Path, "/") + path
	req, err := http.NewRequestWithContext(ctx, method, url.String(), body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.groupToken)
	return req, nil
}

func (e *HTTPError) Error() string {
	if e == nil {
		return "unknown satellite HTTP error"
	}

	if e.Body == "" {
		return fmt.Sprintf(
			"satellite feed returned HTTP %d",
			e.StatusCode,
		)
	}

	return fmt.Sprintf(
		"satellite feed returned HTTP %d: %s",
		e.StatusCode,
		e.Body,
	)
}

func (c *Client) newHTTPError(resp *http.Response) error {
	const maxErrorBodySize = 16 * 1024
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	return &HTTPError{
		StatusCode: resp.StatusCode,
		Body:       strings.TrimSpace(string(body)),
		RetryAfter: resp.Header.Get("Retry-After"),
	}
}

func (s Session) LatestOffset() (int64, error) {
	raw, ok := s["latest_offset"]
	if !ok {
		return 0, errors.New("session is missing latest_offset")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		return 0, fmt.Errorf("decode latest_offset: %w", err)
	}

	var text string
	switch v := value.(type) {
	case json.Number:
		text = v.String()
	case string:
		text = v
	default:
		return 0, fmt.Errorf(
			"latest_offset must be a string or integer, got %T", value,
		)
	}

	offset, err := strconv.ParseInt(text, 10, 64)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid latest_offset %q", text)
	}

	return offset, nil
}
