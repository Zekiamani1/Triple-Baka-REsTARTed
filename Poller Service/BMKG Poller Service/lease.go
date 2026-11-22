package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type pollLease struct {
	source   string
	instance string
	token    atomic.Uint64
	active   atomic.Bool
}

type leaseReply struct {
	Acquired   bool   `json:"acquired"`
	Token      uint64 `json:"fencing_token"`
	Checkpoint string `json:"checkpoint"`
	ServerTime string `json:"server_time"`
}

func newPollLease(source string) *pollLease {
	host, _ := os.Hostname()
	return &pollLease{source: source, instance: host + "-" + uuid.NewString()}
}

func (l *pollLease) update(ctx context.Context, advanceCheckpoint bool) (leaseReply, error) {
	body := map[string]any{
		"source":             l.source,
		"instance_id":        l.instance,
		"advance_checkpoint": advanceCheckpoint,
	}
	if token := l.token.Load(); token != 0 {
		body["fencing_token"] = token
	}
	payload, _ := json.Marshal(body)
	endpoint := strings.TrimRight(os.Getenv("AGGREGATE_INTERNAL_URL"), "/") + "/internal/poller/lease"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return leaseReply{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+os.Getenv("AUDIT_API_TOKEN"))
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return leaseReply{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return leaseReply{}, fmt.Errorf("lease coordinator returned HTTP %d", response.StatusCode)
	}
	var result leaseReply
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return leaseReply{}, err
	}
	if result.Acquired {
		l.token.Store(result.Token)
		l.active.Store(true)
	} else {
		l.active.Store(false)
	}
	return result, nil
}

func (l *pollLease) startHeartbeat(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if l.active.Load() {
					callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if _, err := l.update(callCtx, false); err != nil {
						l.active.Store(false)
						slog.WarnContext(ctx, "poller lease heartbeat failed", "source", l.source, "err", err)
					}
					cancel()
				}
			}
		}
	}()
}

type pollFenceKey string

func withPollerFence(ctx context.Context, lease *pollLease, token uint64) context.Context {
	ctx = context.WithValue(ctx, pollFenceKey("lease"), lease)
	ctx = context.WithValue(ctx, pollFenceKey("instance"), lease.instance)
	return context.WithValue(ctx, pollFenceKey("token"), token)
}

func pollerFence(ctx context.Context) (string, uint64) {
	instance, _ := ctx.Value(pollFenceKey("instance")).(string)
	token, _ := ctx.Value(pollFenceKey("token")).(uint64)
	return instance, token
}

func confirmPollLease(ctx context.Context) error {
	lease, _ := ctx.Value(pollFenceKey("lease")).(*pollLease)
	_, expectedToken := pollerFence(ctx)
	if lease == nil || expectedToken == 0 {
		return fmt.Errorf("poller lease is missing")
	}
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	state, err := lease.update(checkCtx, false)
	if err != nil {
		return err
	}
	if !state.Acquired || state.Token != expectedToken {
		return fmt.Errorf("poller lease was taken over")
	}
	return nil
}
