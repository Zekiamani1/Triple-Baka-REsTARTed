package client

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

var ErrInvalidCredentials = errors.New("client: invalid credentials")

var dummyhash = mustHash("unaa-yaha-yaha-una-ha-ha-ha-unaa")

func mustHash(dummy string) string {
	hash, err := bcrypt.GenerateFromPassword([]byte(dummy), bcrypt.DefaultCost)
	if err != nil {
		panic(err)
	}
	return string(hash)
}

func AuthenticateClient(ctx context.Context, repo Repository, clientID, secret string) (*Client, error) {
	client, err := repo.GetByID(ctx, clientID)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return nil, fmt.Errorf("client lookup: %w", err)
		}
		bcrypt.CompareHashAndPassword([]byte(dummyhash), []byte(secret))
		return nil, ErrInvalidCredentials
	}
	err = bcrypt.CompareHashAndPassword([]byte(client.Secret), []byte(secret))
	if err != nil {
		return nil, ErrInvalidCredentials
	}
	return client, nil
}
