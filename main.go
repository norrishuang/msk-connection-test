package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	writeDuration = 20 * time.Second
	pauseDuration = 10 * time.Minute
	msgsPerSecond = 2
)

func main() {
	brokers := getBrokers()
	topic := getTopic()

	log.Printf("MSK Connection Test (kafka-go / segmentio)")
	log.Printf("Brokers  : %v", brokers)
	log.Printf("Topic    : %s", topic)
	log.Printf("Pattern  : write %v → pause %v → repeat", writeDuration, pauseDuration)

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Printf("Received signal %s, shutting down…", sig)
		cancel()
	}()

	// kafka-go writer — 创建一次，整个生命周期复用（保持长连接）
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    1, // 每条消息立即发送，方便观察
			// IdleTimeout & MetadataTTL 设置为 8 分钟，确保 10 分钟 pause 期间连接不被回收
		Transport: &kafka.Transport{
			IdleTimeout: 15 * time.Minute,
			MetadataTTL: 15 * time.Minute,
		},
	}
	defer func() {
		log.Println("Closing writer…")
		if err := w.Close(); err != nil {
			log.Printf("Error closing writer: %v", err)
		}
	}()

	log.Println("Writer initialized, starting loop…")

	cycle := 0
	for {
		cycle++
		log.Printf("=== Cycle #%d: START writing for %v ===", cycle, writeDuration)

		if err := writePhase(ctx, w, cycle); err != nil {
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
			// next cycle
		}
	}
}

// writePhase sends msgsPerSecond messages per second for writeDuration.
func writePhase(ctx context.Context, w *kafka.Writer, cycle int) error {
	deadline := time.Now().Add(writeDuration)
	interval := time.Second / time.Duration(msgsPerSecond)
	ticker := time.NewTicker(interval)
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
			msg := kafka.Message{
				Key:   []byte(fmt.Sprintf("key-%d-%d", cycle, seq)),
				Value: []byte(value),
			}
			writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
			err := w.WriteMessages(writeCtx, msg)
			writeCancel()
			if err != nil {
				log.Printf("[WARN] cycle=%d seq=%d send error: %v", cycle, seq, err)
				continue
			}
			log.Printf("[SEND] cycle=%d seq=%d ts=%s", cycle, seq, t.UTC().Format("15:04:05.000"))
		}
	}
	log.Printf("=== Cycle #%d: DONE writing %d messages ===", cycle, seq)
	return nil
}

func getBrokers() []string {
	if v := os.Getenv("MSK_BROKERS"); v != "" {
		return splitCSV(v)
	}
	if len(os.Args) > 1 {
		return splitCSV(os.Args[1])
	}
	return []string{"localhost:9092"}
}

func getTopic() string {
	if v := os.Getenv("MSK_TOPIC"); v != "" {
		return v
	}
	return "msk-test"
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
