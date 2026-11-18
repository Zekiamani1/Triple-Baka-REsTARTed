package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	error
	StatusCode int
	Body       string
	RetryAfter string
}

type Event struct {
	Offset string
	Type   string
	Data   json.RawMessage
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
	req, err := c.newRequest(ctx, http.MethodGet, "/v1/stream?from="+url.QueryEscape(from), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		defer resp.Body.Close()
		return nil, fmt.Errorf("failed to perform request: %w", err)
	}

	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	if !strings.HasPrefix(contentType, "event-stream") {
		defer resp.Body.Close()
		return nil, fmt.Errorf("unexpected stream content-type: %q", contentType)
	}
	return resp, nil
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

func (c *Client) newHTTPError(resp *http.Response) error {
	const maxErrorBodySize = 16 * 1024
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	return &HTTPError{
		StatusCode: resp.StatusCode,
		Body:       strings.TrimSpace(string(body)),
		RetryAfter: resp.Header.Get("Retry-After"),
	}
}
