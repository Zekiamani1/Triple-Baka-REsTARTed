package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/Zekiamani1/Triple-Baka-REsTARTed/auth-service/internal/client"
	"github.com/Zekiamani1/Triple-Baka-REsTARTed/auth-service/internal/server"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
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
	database := server.ConnectDBWithRetry(dsn, 10, 2*time.Second)
	defer database.Close()
	repo := client.NewPostgresRepo(database)
	err := client.SeedClients(cont, repo, clients)
	if err != nil {
		log.Fatalf("seed failed: %v", err)
	}
	log.Println("seeding clients done")
}

func getEnv(key string) string {
	envar := os.Getenv(key)
	if envar == "" {
		log.Fatalf("seed: required env var %s is not set", key)
	}
	return envar
}
