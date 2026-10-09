FROM golang:1.27-alpine AS builder

RUN apk add --no-cache build-base

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /out/bmkg .

FROM alpine:3.22

RUN apk add --no-cache ca-certificates

WORKDIR /data
COPY --from=builder /out/bmkg /app/bmkg

EXPOSE 8081

CMD ["/app/bmkg"]
