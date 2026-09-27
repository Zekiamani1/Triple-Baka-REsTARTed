package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/Zekiamani1/Triple-Baka-REsTARTed/auth-service/internal/client"
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
	database := connectWithRetry(dsn, 10, 2*time.Second)
	defer database.Close()
	repo := client.NewPostgresRepo(database)
	err := client.SeedClients(cont, repo, clients)
	if err != nil {
		log.Fatalf("seed failed: %v", err)
	}
	log.Println("seeding clients done")
}

func getEnv(key string) string {
	envar := os.Getenv("key")
	if envar == "" {
		log.Fatalf("seed: required env var %s is not set", key)
	}
	return envar
}

func connectWithRetry(dsn string, attempts int, delay time.Duration) *sql.DB {
	var db *sql.DB
	var err error
	for i := range attempts {
		db, err = sql.Open("pgx", dsn)
		if err != nil {
			log.Printf("seed: db not ready (attempt %d/%d): %v", i+1, attempts, err)
			time.Sleep(delay)
		} else {
			ping, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = db.PingContext(ping)
			cancel()
			if err == nil {
				return db
			}
		}
	}
	log.Fatalf("seed: could not connect to db after %d attempts: %v", attempts, err)
	return nil
}
