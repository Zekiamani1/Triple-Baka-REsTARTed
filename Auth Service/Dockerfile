FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/auth-service ./cmd/server \
 && CGO_ENABLED=0 go build -o /out/seed ./cmd/seed

FROM alpine:latest
WORKDIR /app
COPY --from=builder /out/auth-service /out/seed ./
EXPOSE 8083 8084
CMD ["./auth-service"]