# MSK Connection Test

A Go-based Kafka producer for testing Amazon MSK connectivity and long-lived connection stability.

## Behavior

```
write 20s (2 msg/s = 40 msgs)
  → pause 10 min (connection kept alive, no messages sent)
  → write 20s again
  → repeat indefinitely  (Ctrl+C to stop)
```

## Prerequisites

- Go 1.20+
- Network access to your MSK cluster (security group / VPC routing)
- The target Kafka topic must already exist (or auto-create must be enabled)

## Configuration

| Env variable   | Description                              | Default        |
|----------------|------------------------------------------|----------------|
| `MSK_BROKERS`  | Comma-separated broker endpoints         | `localhost:9092` |
| `MSK_TOPIC`    | Target topic name                        | `msk-test`     |

You can also pass the broker list as the first CLI argument:

```bash
./msk-connection-test "b-1.xxx.kafka.us-east-1.amazonaws.com:9092,b-2.xxx..."
```

## Build & Run

```bash
# build
go build -o msk-connection-test .

# run (plaintext / no auth)
export MSK_BROKERS="b-1.xxx.kafka.us-east-1.amazonaws.com:9092,b-2.xxx.kafka.us-east-1.amazonaws.com:9092"
export MSK_TOPIC="msk-test"
./msk-connection-test
```

## TLS / SASL (if your MSK cluster requires authentication)

Uncomment the TLS/SASL block in `main.go` and set:

```bash
export MSK_SASL_USER="your-username"
export MSK_SASL_PASS="your-password"
```

Supported SASL mechanism: `SCRAM-SHA-512` (most common for MSK with auth enabled).

## MSK Version

Tested against MSK 3.9.x. The producer uses Kafka protocol version `3.6.0` (highest supported by [sarama](https://github.com/IBM/sarama)), which is fully compatible with MSK 3.9.x brokers.
