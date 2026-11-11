package client

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"auth-service/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var ErrNotFound = errors.New("client: not found")

type Repository interface {
	UpsertClient(cont context.Context, cli Client) error
	GetByID(cont context.Context, ID string) (*Client, error)
}
type PostgresRepo struct {
	db *sql.DB
}

func NewPostgresRepo(db *sql.DB) *PostgresRepo {
	return &PostgresRepo{db: db}
}

func (this *PostgresRepo) UpsertClient(cont context.Context, cli Client) error {
	cont, span := otel.Tracer(telemetry.ScopeName).Start(cont, "PostgresRepo.UpsertClient",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "UpsertClient"),
			attribute.String("auth.client_id", cli.ClientID),
		),
	)
	defer span.End()

	start := time.Now()
	const query = `
		INSERT INTO clients (client_id, client_secret, scope, display_name, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (client_id) DO UPDATE
		SET client_secret = EXCLUDED.client_secret,
			scope		  = EXCLUDED.scope,
			display_name  = EXCLUDED.display_name
	`
	createdAt := cli.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err := this.db.ExecContext(cont, query, cli.ClientID, cli.Secret, cli.Scope, cli.DisplayName, createdAt)
	durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
	telemetry.RecordDBQuery(cont, "UpsertClient", durationMs)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
	}
	return err
}

func (this *PostgresRepo) GetByID(cont context.Context, ID string) (*Client, error) {
	cont, span := otel.Tracer(telemetry.ScopeName).Start(cont, "PostgresRepo.GetByID",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("db.system", "postgresql"),
			attribute.String("db.operation", "GetByID"),
			attribute.String("auth.client_id", ID),
		),
	)
	defer span.End()

	start := time.Now()
	const query = `
		SELECT client_id, client_secret, scope, display_name, created_at
		FROM clients
		WHERE client_id = $1
	`
	var client Client
	err := this.db.QueryRowContext(cont, query, ID).Scan(
		&client.ClientID, &client.Secret, &client.Scope, &client.DisplayName, &client.CreatedAt,
	)
	durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
	telemetry.RecordDBQuery(cont, "GetByID", durationMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return nil, err
	}
	return &client, nil
}
