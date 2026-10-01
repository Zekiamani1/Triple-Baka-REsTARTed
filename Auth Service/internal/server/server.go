package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"auth-service/internal/client"
	"auth-service/internal/keys"
	"auth-service/internal/token"
	"auth-service/proto/auth/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Deps struct {
	ClientRepo      client.Repository
	RefreshRepo     token.RefreshTokenRepository
	Keys            *keys.Keys
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration
}

type Server struct {
	authv1.UnimplementedAuthServiceServer
	deps Deps
}

func NewServer(d Deps) *Server {
	return &Server{deps: d}
}

var errUnauth = status.Error(codes.Unauthenticated, "invalid credentials")
var errInternal = status.Error(codes.Internal, "internal error")

func (this *Server) IssueToken(ctx context.Context, request *authv1.IssueTokenRequest) (*authv1.TokenResponse, error) {
	if request.ClientId == "" || request.ClientSecret == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id and client_secret are required")
	}
	cli, err := client.AuthenticateClient(ctx, this.deps.ClientRepo, request.ClientId, request.ClientSecret)
	if err != nil {
		if errors.Is(err, client.ErrInvalidCredentials) {
			slog.ErrorContext(ctx, "auth failed", "client_id", request.ClientId)
			return nil, errUnauth
		}
		slog.ErrorContext(ctx, "authenticate client", "err", err)
		return nil, errInternal
	}
	return this.issuePair(ctx, cli)
}

func (this *Server) RefreshToken(ctx context.Context, request *authv1.RefreshTokenRequest) (*authv1.TokenResponse, error) {
	if request.GetRefreshToken() == "" {
		return nil, status.Error(codes.InvalidArgument, "refresh_token is required")
	}
	cliID, err := this.deps.RefreshRepo.Consume(ctx, token.HashRefreshToken(request.RefreshToken))
	if errors.Is(err, token.ErrInvalidRefreshToken) {
		return nil, errUnauth
	}
	if err != nil {
		slog.ErrorContext(ctx, "consume refresh", "err", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	cli, err := this.deps.ClientRepo.GetByID(ctx, cliID)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			return nil, errUnauth
		}
		slog.ErrorContext(ctx, "fetch client for refresh", "err", err)
		return nil, errInternal
	}
	return this.issuePair(ctx, cli)
}

func (this *Server) issuePair(ctx context.Context, cli *client.Client) (*authv1.TokenResponse, error) {
	accToken, _, err := token.BuildAccessToken(this.deps.Keys, cli, this.deps.AccessTokenTTL)
	if err != nil {
		slog.ErrorContext(ctx, "build access token", "err", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	refreshToken, err := token.GenerateRefreshToken()
	if err != nil {
		slog.ErrorContext(ctx, "generate refresh token", "err", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	err = token.StoreRefreshToken(ctx, this.deps.RefreshRepo, cli.ClientID, token.HashRefreshToken(refreshToken), time.Now().Add(this.deps.RefreshTokenTTL))
	if err != nil {
		slog.ErrorContext(ctx, "store refresh token", "err", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &authv1.TokenResponse{AccessToken: accToken, RefreshToken: refreshToken, ExpiresIn: int64(this.deps.AccessTokenTTL.Seconds())}, nil
}

func (this *Server) GetJWKS(ctx context.Context, _ *authv1.GetJWKSRequest) (*authv1.GetJWKSResponse, error) {
	response := &authv1.GetJWKSResponse{}
	for _, k := range this.deps.Keys.JWKS() {
		response.Keys = append(response.Keys, &authv1.JSONWebKey{
			Kty: k.Kty, Use: k.Use, Alg: k.Alg, Kid: k.Kid, N: k.N, E: k.E,
		})
	}
	return response, nil
}

func ConnectDBWithRetry(ctx context.Context, dsn string, attempts int, delay time.Duration) (*sql.DB, error) {
	conn, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("db: open: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		lastErr = conn.PingContext(pingCtx)
		cancel()
		if lastErr == nil {
			return conn, nil
		}

		slog.Warn("db: not ready yet", "attempt", attempt, "attempts", attempts, "err", lastErr)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			conn.Close()
			return nil, ctx.Err()
		}
	}
	conn.Close()
	return nil, fmt.Errorf("db: unreachable after %d attempts: %w", attempts, lastErr)
}
