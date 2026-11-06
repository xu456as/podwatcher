# syntax=docker/dockerfile:1
FROM golang:1.26.2-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o podwatcher ./cmd/server

FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata logrotate bash

WORKDIR /app

RUN mkdir -p /var/log/podwatcher

COPY --from=builder /app/podwatcher .
COPY docker/config.example.yaml ./config.yaml.example
COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8080

#ENTRYPOINT ["/app/entrypoint.sh"]
ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]
