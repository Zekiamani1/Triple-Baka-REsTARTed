package client

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	ScopeReadSummary = "hazard:read:summary"
	ScopeReadFull    = "hazard:read:full"
)

type Client struct {
	ClientID    string
	Secret      string
	Scope       string
	DisplayName string
	CreatedAt   time.Time
}

type SeedSpec struct {
	ClientID    string
	PlainSecret string
	Scope       string
	DisplayName string
}

func SeedClients(cont context.Context, repo Repository, specs []SeedSpec) error {
	for _, spec := range specs {
		if spec.ClientID == "" || spec.PlainSecret == "" {
			return fmt.Errorf("seed: client %q missing id or secret", spec.ClientID)
		}

		hash, err := bcrypt.GenerateFromPassword([]byte(spec.PlainSecret), bcrypt.DefaultCost)
		if err != nil {
			return fmt.Errorf("seed: hashing secret for %q: %w", spec.ClientID, err)
		}
		client := Client{
			ClientID:    spec.ClientID,
			Scope:       spec.Scope,
			Secret:      string(hash),
			DisplayName: spec.DisplayName,
		}
		if err := repo.UpsertClient(cont, client); err != nil {
			return fmt.Errorf("seed: upserting %q: %w", spec.ClientID, err)
		}
	}

	return nil
}
