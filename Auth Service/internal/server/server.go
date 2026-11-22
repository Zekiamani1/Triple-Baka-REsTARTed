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
	"auth-service/internal/telemetry"
	"auth-service/internal/token"
	"auth-service/proto/auth/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
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
	ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "AuthService.IssueToken")
	defer span.End()

	if request.ClientId == "" || request.ClientSecret == "" {
		telemetry.RecordAuthFailure(ctx, "issue_token", "invalid_argument")
		slog.WarnContext(ctx, "issue token rejected: missing client_id or client_secret")
		err := status.Error(codes.InvalidArgument, "client_id and client_secret are required")
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.String("auth.client_id", request.ClientId))

	cli, err := client.AuthenticateClient(ctx, this.deps.ClientRepo, request.ClientId, request.ClientSecret)
	if err != nil {
		if errors.Is(err, client.ErrInvalidCredentials) {
			telemetry.RecordAuthFailure(ctx, "issue_token", "invalid_credentials")
			slog.WarnContext(ctx, "auth failed: invalid credentials", "client_id", request.ClientId)
			span.RecordError(errUnauth)
			span.SetStatus(otelcodes.Error, "invalid credentials")
			return nil, errUnauth
		}
		telemetry.RecordAuthFailure(ctx, "issue_token", "internal_error")
		slog.ErrorContext(ctx, "authenticate client failed", "client_id", request.ClientId, "err", err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "internal error")
		return nil, errInternal
	}
	return this.issuePair(ctx, cli, "client_credentials")
}

func (this *Server) RefreshToken(ctx context.Context, request *authv1.RefreshTokenRequest) (*authv1.TokenResponse, error) {
	ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "AuthService.RefreshToken")
	defer span.End()

	if request.GetRefreshToken() == "" {
		telemetry.RecordAuthFailure(ctx, "refresh_token", "invalid_argument")
		slog.WarnContext(ctx, "refresh token rejected: missing refresh_token")
		err := status.Error(codes.InvalidArgument, "refresh_token is required")
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return nil, err
	}
	cliID, err := this.deps.RefreshRepo.Consume(ctx, token.HashRefreshToken(request.RefreshToken))
	if errors.Is(err, token.ErrInvalidRefreshToken) {
		telemetry.RecordAuthFailure(ctx, "refresh_token", "invalid_refresh_token")
		slog.WarnContext(ctx, "refresh token failed: invalid or expired token")
		span.RecordError(errUnauth)
		span.SetStatus(otelcodes.Error, "invalid refresh token")
		return nil, errUnauth
	}
	if err != nil {
		telemetry.RecordAuthFailure(ctx, "refresh_token", "internal_error")
		slog.ErrorContext(ctx, "consume refresh token failed", "err", err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "internal error")
		return nil, status.Error(codes.Internal, "internal error")
	}
	span.SetAttributes(attribute.String("auth.client_id", cliID))

	cli, err := this.deps.ClientRepo.GetByID(ctx, cliID)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			telemetry.RecordAuthFailure(ctx, "refresh_token", "client_not_found")
			slog.WarnContext(ctx, "refresh token failed: client not found", "client_id", cliID)
			span.RecordError(errUnauth)
			span.SetStatus(otelcodes.Error, "client not found")
			return nil, errUnauth
		}
		telemetry.RecordAuthFailure(ctx, "refresh_token", "internal_error")
		slog.ErrorContext(ctx, "fetch client for refresh failed", "client_id", cliID, "err", err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "internal error")
		return nil, errInternal
	}
	return this.issuePair(ctx, cli, "refresh_token")
}

func (this *Server) issuePair(ctx context.Context, cli *client.Client, grantType string) (*authv1.TokenResponse, error) {
	ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "AuthService.issuePair")
	defer span.End()
	span.SetAttributes(
		attribute.String("auth.client_id", cli.ClientID),
		attribute.String("auth.scope", cli.Scope),
		attribute.String("auth.grant_type", grantType),
	)

	accToken, _, err := token.BuildAccessToken(this.deps.Keys, cli, this.deps.AccessTokenTTL)
	if err != nil {
		slog.ErrorContext(ctx, "build access token", "err", err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "build access token failed")
		return nil, status.Error(codes.Internal, "internal error")
	}
	refreshToken, err := token.GenerateRefreshToken()
	if err != nil {
		slog.ErrorContext(ctx, "generate refresh token", "err", err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "generate refresh token failed")
		return nil, status.Error(codes.Internal, "internal error")
	}
	err = token.StoreRefreshToken(ctx, this.deps.RefreshRepo, cli.ClientID, token.HashRefreshToken(refreshToken), time.Now().Add(this.deps.RefreshTokenTTL))
	if err != nil {
		slog.ErrorContext(ctx, "store refresh token", "err", err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "store refresh token failed")
		return nil, status.Error(codes.Internal, "internal error")
	}

	telemetry.RecordTokenIssued(ctx, grantType, cli.ClientID, cli.Scope)
	slog.InfoContext(ctx, "token pair issued", "client_id", cli.ClientID, "scope", cli.Scope, "grant_type", grantType)
	return &authv1.TokenResponse{AccessToken: accToken, RefreshToken: refreshToken, ExpiresIn: int64(this.deps.AccessTokenTTL.Seconds())}, nil
}

func (this *Server) GetJWKS(ctx context.Context, _ *authv1.GetJWKSRequest) (*authv1.GetJWKSResponse, error) {
	ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "AuthService.GetJWKS")
	defer span.End()

	telemetry.RecordJWKSRequest(ctx)

	response := &authv1.GetJWKSResponse{}
	for _, k := range this.deps.Keys.JWKS() {
		response.Keys = append(response.Keys, &authv1.JSONWebKey{
			Kty: k.Kty, Use: k.Use, Alg: k.Alg, Kid: k.Kid, N: k.N, E: k.E,
		})
	}
	span.SetAttributes(attribute.Int("auth.jwks.key_count", len(response.Keys)))
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

		slog.WarnContext(ctx, "db: not ready yet", "attempt", attempt, "attempts", attempts, "err", lastErr)
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
