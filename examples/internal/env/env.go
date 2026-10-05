// Package env builds the KafkaConfig shared by the examples from environment variables.
package env

import (
	"os"
	"strings"
	"time"

	kafkaclient "github.com/asabaki/kafka-client"
)

// KafkaConfig reads:
//
//	KAFKA_BOOTSTRAP_SERVERS  comma-separated brokers (default localhost:9092)
//	KAFKA_SASL_USERNAME      enables SASL (SCRAM-SHA-512) and TLS when set
//	KAFKA_SASL_PASSWORD
func KafkaConfig() kafkaclient.KafkaConfig {
	servers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	if servers == "" {
		servers = "localhost:9092"
	}

	cfg := kafkaclient.KafkaConfig{
		BootstrapServers: strings.Split(servers, ","),
		// A zero-value field means zero, not the documented default: set at least these for producers.
		ProducerRequiredAcks: -1,
		ProducerRetryMax:     3,
		ProducerRetryBackoff: 100 * time.Millisecond,
		ProducerTimeout:      10 * time.Second,
		ProducerIdempotent:   true,
		CompressionType:      "lz4",
		// Consumers: rejoin a few times on connection errors before giving up.
		ConsumerGroupMaxRetry: 5,
		SlowMessageThreshold:  5 * time.Second,
	}

	if user := os.Getenv("KAFKA_SASL_USERNAME"); user != "" {
		cfg.SaslEnabled = true
		cfg.SaslMechanisms = "SCRAM-SHA-512"
		cfg.SaslUsername = user
		cfg.SaslPassword = os.Getenv("KAFKA_SASL_PASSWORD")
		cfg.TlsEnabled = true
	}

	return cfg
}
