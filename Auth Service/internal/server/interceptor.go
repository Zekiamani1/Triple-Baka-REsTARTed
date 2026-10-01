package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func Recovery() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "panic in RPC handler",
					"method", info.FullMethod,
					"panic", r,
					"stack", string(debug.Stack()),
				)
				err = status.Error(codes.Internal, "internal error")
				resp = nil
			}
		}()
		return handler(ctx, req)
	}
}

type correlationIDKey struct{}

const correlationHeader = "x-correlation-id"

func Correlation() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get(correlationHeader); len(values) > 0 && values[0] != "" {
				id = values[0]
			}
		}
		if id == "" {
			id = generateCorrelationID()
		}
		ctx = context.WithValue(ctx, correlationIDKey{}, id)
		_ = grpc.SetHeader(ctx, metadata.Pairs(correlationHeader, id))
		return handler(ctx, req)
	}
}

func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey{}).(string)
	return id
}

func generateCorrelationID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		slog.Warn("correlation: crypto/rand.Read failed, using fallback id", "err", err)
		return "unavailable"
	}
	return hex.EncodeToString(buf)
}

func Logging() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		resp, err = handler(ctx, req)
		duration := time.Since(start)

		slog.InfoContext(ctx, "rpc completed",
			"method", info.FullMethod,
			"code", status.Code(err).String(),
			"duration_ms", duration.Milliseconds())
		return resp, err
	}
}

type ContextHandler struct {
	slog.Handler
}

func NewContextHandler(handler slog.Handler) *ContextHandler {
	return &ContextHandler{Handler: handler}
}
func (h *ContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		record.AddAttrs(slog.String("correlation_id", id))
	}
	return h.Handler.Handle(ctx, record)
}
func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{Handler: h.Handler.WithAttrs(attrs)}
}
func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{Handler: h.Handler.WithGroup(name)}
}
