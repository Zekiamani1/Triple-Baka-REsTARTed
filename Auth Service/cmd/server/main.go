package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"auth-service/internal/client"
	"auth-service/internal/keys"
	"auth-service/internal/server"
	"auth-service/internal/token"
	authv1 "auth-service/proto/auth/v1"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type Config struct {
	Port             string        // AUTH_PORT, default "8083" (the gRPC listener)
	DSN              string        // AUTH_DB_DSN, required
	KeyPath          string        // AUTH_JWT_PRIVATE_KEY_PATH, required
	Issuer           string        // AUTH_ISSUER, default "bnpb-auth"
	AccessTokenTTL   time.Duration // AUTH_ACCESS_TOKEN_TTL_SECONDS, default 60s
	RefreshTokenTTL  time.Duration // AUTH_REFRESH_TOKEN_TTL_SECONDS, default 24h
	EnableReflection bool          // AUTH_ENABLE_REFLECTION == "true"
}

func runKeygen() error {
	path := os.Getenv("AUTH_JWT_PRIVATE_KEY_PATH")
	if path == "" {
		return errors.New("AUTH_JWT_PRIVATE_KEY_PATH is not set")
	}
	created, err := keys.EnsureKey(path)
	if err != nil {
		return err
	}
	if created {
		slog.Info("keygen: wrote new signing key", "path", path)
	} else {
		slog.Info("keygen: signing key already present, leaving it alone", "path", path)
	}
	return nil
}

func main() {
	switch {
	case len(os.Args) == 1:
		if err := run(); err != nil {
			slog.Error("auth service stopped", "err", err)
			os.Exit(1)
		}
	case os.Args[1] == "keygen":
		if err := runKeygen(); err != nil {
			slog.Error("keygen failed", "err", err)
			os.Exit(1)
		}
	default:
		slog.Error("unknown command", "command", os.Args[1], "usage", "auth-service [keygen]")
		os.Exit(2)
	}
}

func loadConfig() (*Config, error) {
	var err error
	port := os.Getenv("AUTH_PORT")
	if port == "" {
		port = "8083"
	}
	db := os.Getenv("AUTH_DB_DSN")
	if db == "" {
		return nil, fmt.Errorf("AUTH_DB_DSN not set")
	}
	keypath := os.Getenv("AUTH_JWT_PRIVATE_KEY_PATH")
	if keypath == "" {
		return nil, fmt.Errorf("AUTH_JWT_PRIVATE_KEY_PATH not set")
	}
	issuer := os.Getenv("AUTH_ISSUER")
	if issuer == "" {
		issuer = "bnpb-auth"
	}
	refreshval := 86400
	refreshttl := os.Getenv("AUTH_REFRESH_TOKEN_TTL_SECONDS")
	if refreshttl != "" {
		refreshval, err = strconv.Atoi(refreshttl)
		if err != nil {
			return nil, fmt.Errorf("AUTH_REFRESH_TOKEN_TTL_SECONDS invalid")
		}
	}
	accessval := 60
	accessttl := os.Getenv("AUTH_ACCESS_TOKEN_TTL_SECONDS")
	if accessttl != "" {
		accessval, err = strconv.Atoi(accessttl)
		if err != nil {
			return nil, fmt.Errorf("AUTH_ACCESS_TOKEN_TTL_SECONDS invalid")
		}
	}
	reflection := false
	refstr := os.Getenv("AUTH_ENABLE_REFLECTION")
	if refstr == "true" {
		reflection = true
	}
	return &Config{
		Port:             port,
		DSN:              db,
		KeyPath:          keypath,
		Issuer:           issuer,
		RefreshTokenTTL:  time.Duration(refreshval) * time.Second,
		AccessTokenTTL:   time.Duration(accessval) * time.Second,
		EnableReflection: reflection,
	}, nil
}

func run() error {
	slog.SetDefault(slog.New(server.NewContextHandler(slog.NewJSONHandler(os.Stdout, nil))))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config, err := loadConfig()
	if err != nil {
		return err
	}
	key, err := keys.Load(config.KeyPath)
	if err != nil {
		return err
	}
	db, err := server.ConnectDBWithRetry(ctx, config.DSN, 10, 10*time.Second)
	if err != nil {
		return err
	}
	accessRepo := client.NewPostgresRepo(db)
	refreshRepo := token.NewPostgresRefreshRepo(db)
	authServer := server.NewServer(server.Deps{
		ClientRepo:      accessRepo,
		RefreshRepo:     refreshRepo,
		Keys:            key,
		AccessTokenTTL:  config.AccessTokenTTL,
		RefreshTokenTTL: config.RefreshTokenTTL,
	})
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(
		server.Recovery(),
		server.Correlation(),
		server.Logging(),
	))
	authv1.RegisterAuthServiceServer(grpcServer, authServer)

	healthServer := health.NewServer()
	healthv1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)

	if config.EnableReflection {
		reflection.Register(grpcServer)
		slog.Warn("gRPC reflection is enabled")
	}
	listen, err := net.Listen("tcp", ":"+config.Port)
	if err != nil {
		return err
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(listen) }()

	slog.Info("auth service listening", "grpc_addr", listen.Addr().String())
	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	slog.Info("shutting down")
	shutContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutContext.Done():
		grpcServer.Stop()
	}
	return nil
}
