FROM golang:1.26-alpine AS grpcurl-builder

RUN CGO_ENABLED=0 go install github.com/fullstorydev/grpcurl/cmd/grpcurl@v1.9.4

FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .
COPY --from=grpcurl-builder /go/bin/grpcurl /usr/local/bin/grpcurl
COPY proto/grpc/health/v1/health.proto /app/health.proto

CMD ["npm", "run", "dev"]
