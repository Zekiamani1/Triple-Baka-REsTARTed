FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/auth-service ./cmd/server \
 && CGO_ENABLED=0 go build -o /out/seed ./cmd/seed \
 && CGO_ENABLED=0 go install github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.4

FROM alpine:latest
WORKDIR /app
COPY --from=builder /out/auth-service /out/seed ./
COPY --from=builder /go/bin/grpcurl /usr/local/bin/grpcurl
COPY proto/grpc/health/v1/health.proto /app/health.proto
EXPOSE 8083
CMD ["./auth-service"]
