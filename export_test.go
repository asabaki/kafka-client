package kafkaclient

import "github.com/IBM/sarama"

// WithSaramaConfigHookForTest adjusts the sarama config built by ToSaramaConfig. Tests only.
func WithSaramaConfigHookForTest(hook func(*sarama.Config)) KafkaConfigOption {
	return kafkaConfigOptionFunc(func(cfg *KafkaConfig) {
		cfg.saramaConfigHook = hook
	})
}
