package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
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

	log.Printf("MSK Connection Test (segmentio/kafka-go)")
	log.Printf("TEST_MODE      : %s", testMode)
	log.Printf("Brokers        : %s", strings.Join(brokers, ", "))
	log.Printf("Topic          : %s", topic)
	log.Printf("PAUSE_DURATION : %v", pauseDur)
	log.Printf("Pattern        : write %v → pause %v → repeat", writeDuration, pauseDur)

	// Build transport based on TEST_MODE
	var transport *kafka.Transport
	switch testMode {
	case "with-keepalive":
		dialer := &net.Dialer{
			KeepAlive: 60 * time.Second,
		}
		transport = &kafka.Transport{
			Dial:        dialer.DialContext,
			IdleTimeout: 280 * time.Second,
			MetadataTTL: 15 * time.Minute,
		}
		log.Println("Keepalive ENABLED: TCP KeepAlive=60s, IdleTimeout=280s")
	default: // "no-keepalive"
		transport = &kafka.Transport{
			IdleTimeout: 15 * time.Minute,
			MetadataTTL: 15 * time.Minute,
		}
		log.Println("Keepalive DISABLED: IdleTimeout=15m (reproducing 350s timeout)")
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(brokers...),
		Topic:     topic,
		Balancer:  &kafka.RoundRobin{},
		Transport: transport,
	}
	defer writer.Close()

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("Received signal %s, shutting down…", sig)
		cancel()
	}()

	log.Println("Producer initialized, starting loop…")

	for cycle := 1; ; cycle++ {
		log.Printf("=== Cycle #%d: START writing for %v ===", cycle, writeDuration)
		if writePhase(ctx, writer, topic, cycle) {
			return
		}

		log.Printf("=== Cycle #%d: PAUSE for %v (connection idle) ===", cycle, pauseDur)
		select {
		case <-ctx.Done():
			log.Println("Shutting down during pause.")
			return
		case <-time.After(pauseDur):
		}
		log.Printf("=== Cycle #%d: PAUSE ended, resuming writes ===", cycle)
	}
}

func writePhase(ctx context.Context, w *kafka.Writer, topic string, cycle int) bool {
	deadline := time.Now().Add(writeDuration)
	interval := time.Second / time.Duration(msgsPerSecond)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	seq := 0
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			log.Println("Shutting down during write phase.")
			return true
		case t := <-ticker.C:
			seq++
			value := fmt.Sprintf(
				`{"cycle":%d,"seq":%d,"ts":"%s","note":"msk-connection-test"}`,
				cycle, seq, t.UTC().Format(time.RFC3339Nano),
			)
			err := w.WriteMessages(ctx, kafka.Message{
				Key:   []byte(fmt.Sprintf("key-%d-%d", cycle, seq)),
				Value: []byte(value),
			})
			if err != nil {
				classifyAndLogError(err, cycle, seq)
			} else {
				log.Printf("[SEND-OK] cycle=%d seq=%d", cycle, seq)
			}
		}
	}
	log.Printf("=== Cycle #%d: DONE writing %d messages ===", cycle, seq)
	return false
}

func classifyAndLogError(err error, cycle, seq int) {
	errStr := err.Error()
	tag := "[SEND-ERROR]"
	if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline") {
		tag = "[TIMEOUT-ERROR]"
	} else if strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "refused") {
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

func getBrokers() []string {
	s := os.Getenv("MSK_BROKERS")
	if s == "" && len(os.Args) > 1 {
		s = os.Args[1]
	}
	if s == "" {
		s = "localhost:9092"
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func getTopic() string {
	if v := os.Getenv("MSK_TOPIC"); v != "" {
		return v
	}
	return "msk-test"
}
