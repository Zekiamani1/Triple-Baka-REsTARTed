package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

type leaseAcquireResponse struct {
	Acquired     bool   `json:"acquired"`
	LeaderID     string `json:"leader_id"`
	FencingToken int64  `json:"fencing_token"`
	Cursor       string `json:"cursor"`
}

func pollerInstanceID() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "pvmbg-poller-local"
}

func leaseTTLSeconds() int {
	v, err := strconv.Atoi(envOrDefault("LEASE_TTL_SECONDS", "15"))
	if err != nil || v < 5 {
		return 15
	}
	return v
}

func acquireLease(ctx context.Context, source, instanceID string) (leaseAcquireResponse, error) {
	base := envOrDefault("AGGREGATE_HTTP_URL", "http://aggregate-http:50052")
	payload, _ := json.Marshal(map[string]any{
		"source":      source,
		"instance_id": instanceID,
		"ttl_seconds": leaseTTLSeconds(),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/internal/leases/acquire", bytes.NewReader(payload))
	if err != nil {
		return leaseAcquireResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return leaseAcquireResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return leaseAcquireResponse{}, fmt.Errorf("acquire lease HTTP %d", resp.StatusCode)
	}
	var out leaseAcquireResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return leaseAcquireResponse{}, err
	}
	return out, nil
}

func advanceLeaseCursor(ctx context.Context, source, instanceID string, token int64, cursor time.Time) error {
	base := envOrDefault("AGGREGATE_HTTP_URL", "http://aggregate-http:50052")
	payload, _ := json.Marshal(map[string]any{
		"source":        source,
		"instance_id":   instanceID,
		"fencing_token": token,
		"cursor":        cursor.UTC().Format(time.RFC3339),
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/internal/leases/cursor", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("advance cursor HTTP %d", resp.StatusCode)
	}
	return nil
}
