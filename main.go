package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	writeDuration = 20 * time.Second
	pauseDuration = 10 * time.Minute
	msgsPerSecond = 2
)

func main() {
	// MSK broker addresses — set via env or command-line arg
	brokers := getBrokers()
	topic := getTopic()

	log.Printf("MSK Connection Test")
	log.Printf("Brokers  : %v", brokers)
	log.Printf("Topic    : %s", topic)
	log.Printf("Pattern  : write %v → pause %v → repeat", writeDuration, pauseDuration)

	// Sarama config for MSK (Kafka 3.9.x)
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0 // highest version supported by sarama; compatible with MSK 3.9.x
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Net.DialTimeout = 10 * time.Second
	cfg.Net.ReadTimeout = 30 * time.Second
	cfg.Net.WriteTimeout = 10 * time.Second
	cfg.Metadata.RefreshFrequency = 5 * time.Minute

	// TLS/SASL — uncomment and configure if your MSK cluster uses authentication
	// cfg.Net.TLS.Enable = true
	// cfg.Net.TLS.Config = &tls.Config{InsecureSkipVerify: false}
	// cfg.Net.SASL.Enable = true
	// cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	// cfg.Net.SASL.User = os.Getenv("MSK_SASL_USER")
	// cfg.Net.SASL.Password = os.Getenv("MSK_SASL_PASS")

	log.Println("Connecting to MSK cluster…")
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Error closing producer: %v", err)
		}
	}()
	log.Println("Connected ✓")

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("Received signal %s, shutting down…", sig)
		cancel()
	}()

	cycle := 0
	for {
		cycle++
		log.Printf("=== Cycle #%d: START writing for %v ===", cycle, writeDuration)
		if err := writePhase(ctx, producer, topic, cycle); err != nil {
			if ctx.Err() != nil {
				log.Println("Context cancelled, exiting.")
				return
			}
			log.Printf("Write error: %v", err)
		}

		log.Printf("=== Cycle #%d: PAUSE for %v (connection kept alive) ===", cycle, pauseDuration)
		select {
		case <-ctx.Done():
			log.Println("Context cancelled during pause, exiting.")
			return
		case <-time.After(pauseDuration):
			// continue to next cycle
		}
	}
}

// writePhase sends msgsPerSecond messages every second for writeDuration.
func writePhase(ctx context.Context, producer sarama.SyncProducer, topic string, cycle int) error {
	deadline := time.Now().Add(writeDuration)
	ticker := time.NewTicker(time.Second / msgsPerSecond)
	defer ticker.Stop()

	seq := 0
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case t := <-ticker.C:
			seq++
			value := fmt.Sprintf(
				`{"cycle":%d,"seq":%d,"ts":"%s","note":"msk-connection-test"}`,
				cycle, seq, t.UTC().Format(time.RFC3339Nano),
			)
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("key-%d-%d", cycle, seq)),
				Value: sarama.StringEncoder(value),
			}
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("[WARN] cycle=%d seq=%d send error: %v", cycle, seq, err)
				continue
			}
			log.Printf("[SEND] cycle=%d seq=%d partition=%d offset=%d", cycle, seq, partition, offset)
		}
	}
	log.Printf("=== Cycle #%d: DONE writing %d messages ===", cycle, seq)
	return nil
}

// getBrokers reads broker addresses from env MSK_BROKERS (comma-separated)
// or falls back to the first command-line argument.
func getBrokers() []string {
	if v := os.Getenv("MSK_BROKERS"); v != "" {
		return splitCSV(v)
	}
	if len(os.Args) > 1 {
		return splitCSV(os.Args[1])
	}
	// default placeholder — replace before running
	return []string{"localhost:9092"}
}

// getTopic reads the topic from env MSK_TOPIC or defaults to "msk-test".
func getTopic() string {
	if v := os.Getenv("MSK_TOPIC"); v != "" {
		return v
	}
	return "msk-test"
}

func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			if t := trim(s[start:i]); t != "" {
				out = append(out, t)
			}
			start = i + 1
		}
	}
	if t := trim(s[start:]); t != "" {
		out = append(out, t)
	}
	return out
}

func trim(s string) string {
	i, j := 0, len(s)
	for i < j && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	for j > i && (s[j-1] == ' ' || s[j-1] == '\t') {
		j--
	}
	return s[i:j]
}
