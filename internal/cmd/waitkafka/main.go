// Command waitkafka blocks until the broker in KAFKA_BOOTSTRAP_SERVERS (default localhost:9092) answers metadata
// requests, or fails after 2 minutes. CI uses it before running the integration tests.
package main

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	servers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if servers == "" {
		servers = "localhost:9092"
	}

	cfg := sarama.NewConfig()
	cfg.Net.DialTimeout = 2 * time.Second
	cfg.Metadata.Retry.Max = 0

	deadline := time.Now().Add(2 * time.Minute)
	for {
		client, err := sarama.NewClient(strings.Split(servers, ","), cfg)
		if err == nil {
			_, err = client.Controller()
			_ = client.Close()
			if err == nil {
				log.Print("kafka is ready")
				return
			}
		}
		if time.Now().After(deadline) {
			log.Fatalf("kafka not ready after 2 minutes: %s", err)
		}
		time.Sleep(2 * time.Second)
	}
}
