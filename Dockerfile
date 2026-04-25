FROM golang:1.21-alpine AS builder

RUN apk add --no-cache build-base librdkafka-dev pkgconfig

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=1 go build -tags musl -ldflags="-w -s" -o msk-connection-test .

# ---- runtime image ----
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/msk-connection-test .

ENV MSK_BROKERS="localhost:9092"
ENV MSK_TOPIC="msk-test"
ENV TEST_MODE="no-keepalive"
ENV PAUSE_DURATION="6m"

ENTRYPOINT ["/app/msk-connection-test"]
