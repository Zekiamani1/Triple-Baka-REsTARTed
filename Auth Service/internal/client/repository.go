package client

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

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
	_, err := this.db.ExecContext(cont, query, cli.ClientID, cli.Secret, cli.Scope, cli.DisplayName, cli.CreatedAt)
	return err
}

func (this *PostgresRepo) GetByID(cont context.Context, ID string) (*Client, error) {
	const query = `
		SELECT client_id, client_secret, scope, display_name, created_at
		FROM clients
		WHERE client_id = $1
	`
	var client Client
	err := this.db.QueryRowContext(cont, query, ID).Scan(
		&client.ClientID, &client.Secret, &client.Scope, &client.DisplayName, &client.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("client: not found")
	}
	if err != nil {
		return nil, err
	}
	return &client, nil
}
