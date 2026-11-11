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

	"auth-service/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "PostgresRefreshRepo.Store",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "StoreRefreshToken"),
			attribute.String("auth.client_id", clientID),
		),
	)
	defer span.End()

	start := time.Now()
	const query = `
		INSERT INTO refresh_tokens (client_id, token_hash, expires_at)
		VALUES ($1, $2, $3)
	`
	_, err := this.db.ExecContext(ctx, query, clientID, tokenHash, expiresAt.UTC())
	durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
	telemetry.RecordDBQuery(ctx, "StoreRefreshToken", durationMs)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
	}
	return err
}

func (this *PostgresRefreshRepo) Consume(ctx context.Context, tokenHash string) (string, error) {
	ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "PostgresRefreshRepo.Consume",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "ConsumeRefreshToken"),
		),
	)
	defer span.End()

	start := time.Now()
	const query = `
		UPDATE refresh_tokens SET revoked_at = NOW()
		WHERE token_hash = $1 AND revoked_at IS NULL AND expires_at > NOW()
		RETURNING client_id
	`
	var id string
	err := this.db.QueryRowContext(ctx, query, tokenHash).Scan(&id)
	durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
	telemetry.RecordDBQuery(ctx, "ConsumeRefreshToken", durationMs)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrInvalidRefreshToken
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
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
