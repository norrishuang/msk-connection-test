package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	writeDuration = 20 * time.Second
	msgsPerSecond = 2
)

func main() {
	brokers := getBrokers()
	topic := getTopic()
	testMode := getTestMode()
	pauseDur := getPauseDuration()

	log.Printf("MSK Connection Test (confluent-kafka-go / librdkafka)")
	log.Printf("TEST_MODE      : %s", testMode)
	log.Printf("Brokers        : %s", brokers)
	log.Printf("Topic          : %s", topic)
	log.Printf("PAUSE_DURATION : %v", pauseDur)
	log.Printf("Pattern        : write %v → pause %v → repeat", writeDuration, pauseDur)

	cfg := buildConfig(brokers, testMode)
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Graceful shutdown
	done := make(chan struct{})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("Received signal %s, shutting down…", sig)
		close(done)
	}()

	log.Println("Producer initialized, starting loop…")

	for cycle := 1; ; cycle++ {
		log.Printf("=== Cycle #%d: START writing for %v ===", cycle, writeDuration)
		if writePhase(producer, topic, cycle, done) {
			return
		}

		log.Printf("=== Cycle #%d: PAUSE for %v (connection idle) ===", cycle, pauseDur)
		select {
		case <-done:
			log.Println("Shutting down during pause.")
			return
		case <-time.After(pauseDur):
		}
		log.Printf("=== Cycle #%d: PAUSE ended, resuming writes ===", cycle)
	}
}

func buildConfig(brokers, testMode string) *kafka.ConfigMap {
	switch testMode {
	case "prod-like":
		log.Println("TEST_MODE: prod-like (simulating ca-balance production config)")
		return &kafka.ConfigMap{
			"bootstrap.servers":                     brokers,
			"client.id":                             "confluent-producer",
			"acks":                                  "all",
			"retries":                               3,
			"linger.ms":                             1,
			"compression.type":                      "snappy",
			"max.in.flight.requests.per.connection": 5,
			"request.timeout.ms":                    30000,
			"delivery.timeout.ms":                   120000,
			"retry.backoff.ms":                      200,
			"batch.size":                            65536,
			"queue.buffering.max.kbytes":            65536,
		}
	case "with-keepalive":
		log.Println("TEST_MODE: with-keepalive (ca-balance config + socket.keepalive.enable=true + connections.max.idle.ms=280000)")
		return &kafka.ConfigMap{
			"bootstrap.servers":                     brokers,
			"client.id":                             "confluent-producer",
			"acks":                                  "all",
			"retries":                               3,
			"linger.ms":                             1,
			"compression.type":                      "snappy",
			"max.in.flight.requests.per.connection": 5,
			"request.timeout.ms":                   30000,
			"delivery.timeout.ms":                  120000,
			"retry.backoff.ms":                      200,
			"batch.size":                            65536,
			"queue.buffering.max.kbytes":            65536,
			// 修复项：在 prod-like 基础上加上这两个参数
			"socket.keepalive.enable":               true,
			"connections.max.idle.ms":               280000,
		}
	default: // "no-keepalive"
		log.Println("Keepalive DISABLED (reproducing 350s timeout)")
		return &kafka.ConfigMap{
			"bootstrap.servers": brokers,
			"client.id":         "msk-test-no-keepalive",
			"acks":              "all",
		}
	}
}

func writePhase(p *kafka.Producer, topic string, cycle int, done chan struct{}) bool {
	deadline := time.Now().Add(writeDuration)
	interval := time.Second / time.Duration(msgsPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	seq := 0
	for time.Now().Before(deadline) {
		select {
		case <-done:
			log.Println("Shutting down during write phase.")
			return true
		case t := <-ticker.C:
			seq++
			value := fmt.Sprintf(
				`{"cycle":%d,"seq":%d,"ts":"%s","note":"msk-connection-test"}`,
				cycle, seq, t.UTC().Format(time.RFC3339Nano),
			)
			err := p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Key:            []byte(fmt.Sprintf("key-%d-%d", cycle, seq)),
				Value:          []byte(value),
			}, nil)
			if err != nil {
				classifyAndLogError(err, cycle, seq)
				continue
			}
			// Wait for delivery report
			e := <-p.Events()
			if m, ok := e.(*kafka.Message); ok {
				if m.TopicPartition.Error != nil {
					classifyAndLogError(m.TopicPartition.Error, cycle, seq)
				} else {
					log.Printf("[SEND-OK] cycle=%d seq=%d", cycle, seq)
				}
			}
		}
	}
	log.Printf("=== Cycle #%d: DONE writing %d messages ===", cycle, seq)
	return false
}

func classifyAndLogError(err error, cycle, seq int) {
	errStr := err.Error()
	tag := "[SEND-ERROR]"
	if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline") ||
		strings.Contains(errStr, "Timed out") {
		tag = "[TIMEOUT-ERROR]"
	} else if strings.Contains(errStr, "disconnect") || strings.Contains(errStr, "Disconnected") ||
		strings.Contains(errStr, "connection reset") || strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "Transport failure") || strings.Contains(errStr, "refused") {
		tag = "[DISCONNECT-ERROR]"
	}
	log.Printf("%s cycle=%d seq=%d error=%v", tag, cycle, seq, err)
}

func getTestMode() string {
	if v := os.Getenv("TEST_MODE"); v != "" {
		return v
	}
	return "no-keepalive"
}

func getPauseDuration() time.Duration {
	if v := os.Getenv("PAUSE_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("Invalid PAUSE_DURATION %q: %v", v, err)
		}
		return d
	}
	return 10 * time.Minute
}

func getBrokers() string {
	s := os.Getenv("MSK_BROKERS")
	if s == "" && len(os.Args) > 1 {
		s = os.Args[1]
	}
	if s == "" {
		s = "localhost:9092"
	}
	return s
}

func getTopic() string {
	if v := os.Getenv("MSK_TOPIC"); v != "" {
		return v
	}
	return "msk-test"
}
