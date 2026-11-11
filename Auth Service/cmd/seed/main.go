package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"auth-service/internal/client"
	"auth-service/internal/server"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	slog.SetDefault(slog.New(server.NewContextHandler(slog.NewJSONHandler(os.Stdout, nil))))
	cont := context.Background()
	clients := []client.SeedSpec{
		{
			ClientID:    "media",
			PlainSecret: os.Getenv("CLIENT_MEDIA_SECRET"),
			Scope:       "hazard:read:summary",
			DisplayName: "Media",
		},
		{
			ClientID:    "lapangan",
			PlainSecret: os.Getenv("CLIENT_LAPANGAN_SECRET"),
			Scope:       "hazard:read:full",
			DisplayName: "Tim Lapangan",
		},
		{
			ClientID:    "internal",
			PlainSecret: os.Getenv("CLIENT_INTERNAL_SECRET"),
			Scope:       "hazard:read:full",
			DisplayName: "BNPB Pusat / Internal Ops",
		},
	}
	dsn := getEnv("AUTH_DB_DSN")
	database, err := server.ConnectDBWithRetry(cont, dsn, 10, 2*time.Second)
	if err != nil {
		slog.ErrorContext(cont, "connecting to db failed", "err", err)
		os.Exit(1)
	}
	defer database.Close()
	repo := client.NewPostgresRepo(database)
	err = client.SeedClients(cont, repo, clients)
	if err != nil {
		slog.ErrorContext(cont, "seed failed", "err", err)
		os.Exit(1)
	}
	slog.InfoContext(cont, "seeding clients done")
}

func getEnv(key string) string {
	envar := os.Getenv(key)
	if envar == "" {
		slog.Error("seed: required env var is not set", "env", key)
		os.Exit(1)
	}
	return envar
}
