FROM golang:1.27-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/coastal-poller .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/coastal-poller /app/coastal-poller

CMD ["/app/coastal-poller"]
