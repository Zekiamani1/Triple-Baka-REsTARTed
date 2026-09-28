package token

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"
)

var ErrInvalidRefreshToken = errors.New("token: invalid refresh token")

type RefreshTokenRepository interface {
	Store(ctx context.Context, tokenHash, clientID string, expiresAt time.Time) error
	Consume(ctx context.Context, tokenHash string) (string, error)
}

type PostgresRefreshRepo struct {
	db *sql.DB
}

func NewPostgresRefreshRepo(db *sql.DB) *PostgresRefreshRepo {
	return &PostgresRefreshRepo{db: db}
}

func (this *PostgresRefreshRepo) Store(ctx context.Context, tokenHash, clientID string, expiresAt time.Time) error {
	const query = `
		INSERT INTO refresh_tokens (client_id, token_hash, expires_at)
		VALUES ($1, $2, $3)
	`
	_, err := this.db.ExecContext(ctx, query, clientID, tokenHash, expiresAt.UTC())
	return err
}

func (this *PostgresRefreshRepo) Consume(ctx context.Context, tokenHash string) (string, error) {
	const query = `
		UPDATE refresh_tokens SET revoked_at = NOW()
		WHERE token_hash = $1 AND revoked_at IS NULL AND expires_at > NOW()
		RETURNING client_id
	`
	var id string
	err := this.db.QueryRowContext(ctx, query, tokenHash).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrInvalidRefreshToken
	}
	return id, err
}

func GenerateRefreshToken() (plaintext string, err error) {
	bytes := make([]byte, 32)
	_, err = rand.Read(bytes)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}

func HashRefreshToken(plaintext string) string {
	hash := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(hash[:])
}

func StoreRefreshToken(ctx context.Context, repo RefreshTokenRepository, clientID, tokenHash string, expiresAt time.Time) error {
	err := repo.Store(ctx, tokenHash, clientID, expiresAt)
	return err
}
