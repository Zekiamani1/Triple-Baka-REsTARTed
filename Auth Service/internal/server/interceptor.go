package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"auth-service/internal/telemetry"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type metadataCarrier metadata.MD

func (mc metadataCarrier) Get(key string) string {
	vals := metadata.MD(mc).Get(key)
	if len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (mc metadataCarrier) Set(key, val string) {
	metadata.MD(mc).Set(key, val)
}

func (mc metadataCarrier) Keys() []string {
	keys := make([]string, 0, len(mc))
	for k := range mc {
		keys = append(keys, k)
	}
	return keys
}

func Telemetry() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = otel.GetTextMapPropagator().Extract(ctx, metadataCarrier(md))
		}

		ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, info.FullMethod,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.method", info.FullMethod),
			),
		)
		defer span.End()

		start := time.Now()
		resp, err = handler(ctx, req)
		durationMs := float64(time.Since(start).Nanoseconds()) / 1e6

		grpcCode := status.Code(err).String()
		span.SetAttributes(attribute.String("rpc.grpc.status_code", grpcCode))
		if id := CorrelationID(ctx); id != "" {
			span.SetAttributes(attribute.String("correlation_id", id))
		}

		if err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, err.Error())
		} else {
			span.SetStatus(otelcodes.Ok, "")
		}

		if !strings.HasPrefix(info.FullMethod, "/grpc.health.") {
			telemetry.RecordRPC(ctx, info.FullMethod, grpcCode, durationMs)
		}

		return resp, err
	}
}

func Recovery() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				if span := trace.SpanFromContext(ctx); span.IsRecording() {
					span.RecordError(fmt.Errorf("panic: %v", r))
					span.SetStatus(otelcodes.Error, "panic in RPC handler")
				}
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
	otelHandler slog.Handler
}

func NewContextHandler(handler slog.Handler) *ContextHandler {
	return &ContextHandler{
		Handler:     handler,
		otelHandler: otelslog.NewHandler(telemetry.ScopeName),
	}
}
func (h *ContextHandler) Handle(ctx context.Context, record slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		record.AddAttrs(slog.String("correlation_id", id))
	}
	otelRecord := record.Clone()
	if spanCtx := trace.SpanContextFromContext(ctx); spanCtx.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", spanCtx.TraceID().String()),
			slog.String("span_id", spanCtx.SpanID().String()),
		)
	}
	_ = h.otelHandler.Handle(ctx, otelRecord)
	return h.Handler.Handle(ctx, record)
}
func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{
		Handler:     h.Handler.WithAttrs(attrs),
		otelHandler: h.otelHandler.WithAttrs(attrs),
	}
}
func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{
		Handler:     h.Handler.WithGroup(name),
		otelHandler: h.otelHandler.WithGroup(name),
	}
}
