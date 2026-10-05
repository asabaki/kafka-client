package kafkaclient_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/asabaki/kafka-client"
)

type RetryITSuite struct {
	suite.Suite
	kafkaCfg     kafkaclient.KafkaConfig
	producer     sarama.SyncProducer
	admin        sarama.ClusterAdmin
	batchSize    uint16
	batchTimeout time.Duration
}

type retryTestConfig struct {
	name             string
	topic            string
	expectedAttempts int
	retryConfig      kafkaclient.RetryConfig
	setupMessages    func()
	processed        chan struct{}
	consumer         kafkaclient.ConsumerGroup
	attempts         *atomic.Int32
}

func TestRetryITSuite(t *testing.T) {
	requireKafka(t)

	kafkaCfg := makeKafkaConfigForTest()

	suite.Run(t, &RetryITSuite{
		kafkaCfg:     kafkaCfg,
		producer:     createProducerForTest(t, kafkaCfg.BootstrapServers),
		admin:        createClusterAdmin(t, kafkaCfg.BootstrapServers),
		batchSize:    3,
		batchTimeout: 2 * time.Second,
	})
}

func (s *RetryITSuite) produceMessages(topic string, n uint16) {
	for i := 0; i < int(n); i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(fmt.Sprintf("message-%d", i)),
		}
		_, _, err := s.producer.SendMessage(msg)
		s.Require().NoError(err)
		log.Printf("publish mock message: %#v", msg)
	}
}

func (s *RetryITSuite) newRetryConsumer(
	topic string,
	handler kafkaclient.MessageHandler[string],
	retryCfg kafkaclient.RetryConfig,
) kafkaclient.ConsumerGroup {
	groupID := fmt.Sprintf("%s-%s", topic, getNextSequence())
	consumer, err := kafkaclient.NewConsumerGroup(s.kafkaCfg, groupID, topic, false, kafkaclient.WrapWithRetryBackoffHandler(kafkaclient.WrapWithSaramaMessageHandler(handler, simpleStringConverter), retryCfg))
	s.Require().NoError(err)
	log.Printf("consumer group created: group_id=%s topic=%s", groupID, topic)
	return consumer
}

func (s *RetryITSuite) newRetryBatchConsumer(
	topic string,
	handler kafkaclient.MessagesHandler[string],
	retryCfg kafkaclient.RetryConfig,
) kafkaclient.ConsumerGroup {
	groupID := fmt.Sprintf("%s-%s", topic, getNextSequence())
	consumer, err := kafkaclient.NewBatchConsumerGroup(s.kafkaCfg, groupID, topic, s.batchSize, s.batchTimeout, false, kafkaclient.WrapWithRetryBackoffBatchHandler(kafkaclient.WrapWithSaramaMessagesHandler(handler, simpleStringConverter), retryCfg))
	s.Require().NoError(err)
	log.Printf("consumer group created: group_id=%s topic=%s batch_size=%d batch_timeout=%s", groupID, topic, s.batchSize, s.batchTimeout)
	return consumer
}

func (s *RetryITSuite) runRetryTest(t *testing.T, cfg retryTestConfig) {
	cfg.setupMessages()
	consumer := cfg.consumer
	processed := cfg.processed
	attempts := cfg.attempts

	err := consumer.Start()
	assert.NoError(t, err)
	defer consumer.Close()
	assert.Eventually(t, consumer.IsRunning, 10*time.Second, 50*time.Millisecond)

	processAttempt := 0
	timeout := time.After(6 * time.Second)
	gracePeriod := time.Duration(0)
	for {
		select {
		case <-processed:
			processAttempt++
			if processAttempt == cfg.expectedAttempts && gracePeriod == 0 {
				gracePeriod = 1 * time.Second
				timeout = time.After(gracePeriod)
			} else if processAttempt > cfg.expectedAttempts {
				assert.Fail(t, "Retry too many times", "expected %d attempts, got %d", cfg.expectedAttempts, processAttempt)
				return
			}
		case <-timeout:
			if processAttempt == cfg.expectedAttempts {
				assert.Equal(t, cfg.expectedAttempts, int(attempts.Load()), "should have correct number of attempts")
			} else {
				assert.Failf(t, "Incorrect number of retries", "expected %d attempts, got %d", cfg.expectedAttempts, processAttempt)
			}
			return
		}
	}
}

func (s *RetryITSuite) defaultRetryConfig() kafkaclient.RetryConfig {
	return kafkaclient.RetryConfig{
		MaxRetries: 3,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   1 * time.Second,
	}
}

func (s *RetryITSuite) TestRetry() {
	tests := []struct {
		name             string
		topic            string
		expectedAttempts int
		retryConfig      kafkaclient.RetryConfig
		handler          func(attempts *atomic.Int32, processed chan<- struct{}) kafkaclient.MessageHandler[string]
		messageCount     uint16
	}{
		{
			name:             "retries until success",
			topic:            "test-retry-1",
			expectedAttempts: 3,
			retryConfig:      s.defaultRetryConfig(),
			handler: func(attempts *atomic.Int32, processed chan<- struct{}) kafkaclient.MessageHandler[string] {
				return func(ctx context.Context, msg string) error {
					currentAttempt := attempts.Add(1)
					processed <- struct{}{}
					if currentAttempt <= 2 {
						return errors.New("simulated failure")
					}
					return nil
				}
			},
			messageCount: 1,
		},
		{
			name:             "fails after max retries",
			topic:            "test-retry-2",
			expectedAttempts: 8,
			retryConfig:      s.defaultRetryConfig(),
			handler: func(attempts *atomic.Int32, processed chan<- struct{}) kafkaclient.MessageHandler[string] {
				return func(ctx context.Context, msg string) error {
					attempts.Add(1)
					processed <- struct{}{}
					return errors.New("permanent failure")
				}
			},
			messageCount: 2,
		},
		{
			name:             "respects retry condition",
			topic:            "test-retry-condition",
			expectedAttempts: 2,
			retryConfig: func() kafkaclient.RetryConfig {
				cfg := s.defaultRetryConfig()
				retryableErr := fmt.Errorf("temporary error")
				cfg.RetryCondition = func(err error) bool {
					return err.Error() == retryableErr.Error()
				}
				return cfg
			}(),
			handler: func(attempts *atomic.Int32, processed chan<- struct{}) kafkaclient.MessageHandler[string] {
				return func(ctx context.Context, msg string) error {
					currentAttempt := attempts.Add(1)
					processed <- struct{}{}
					if currentAttempt == 1 {
						return fmt.Errorf("temporary error")
					}
					return fmt.Errorf("permanent error")
				}
			},
			messageCount: 1,
		},
	}

	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			topic := createTestTopic(t, s.admin, tt.topic)
			var attempts atomic.Int32
			processed := make(chan struct{}, 1)

			consumer := s.newRetryConsumer(topic, tt.handler(&attempts, processed), tt.retryConfig)

			s.runRetryTest(t, retryTestConfig{
				name:             tt.name,
				topic:            topic,
				expectedAttempts: tt.expectedAttempts,
				retryConfig:      tt.retryConfig,
				setupMessages:    func() { s.produceMessages(topic, tt.messageCount) },
				consumer:         consumer,
				attempts:         &attempts,
				processed:        processed,
			})
		})
	}
}

func (s *RetryITSuite) TestBatchRetry() {
	tests := []struct {
		name             string
		topic            string
		expectedAttempts int
		retryConfig      kafkaclient.RetryConfig
		handler          func(attempts *atomic.Int32, processed chan<- struct{}) kafkaclient.MessagesHandler[string]
		messageCount     uint16
	}{
		{
			name:             "retries until success",
			topic:            "test-batch-retry-1",
			expectedAttempts: 3,
			retryConfig:      s.defaultRetryConfig(),
			handler: func(attempts *atomic.Int32, processed chan<- struct{}) kafkaclient.MessagesHandler[string] {
				return func(messages []*kafkaclient.MessageWithContext[string]) error {
					currentAttempt := attempts.Add(1)
					processed <- struct{}{}
					if int(currentAttempt) <= 2 {
						return errors.New("simulated failure")
					}
					return nil
				}
			},
			messageCount: 3,
		},
		{
			name:             "fails after max retries",
			topic:            "test-batch-retry-2",
			expectedAttempts: 8,
			retryConfig:      s.defaultRetryConfig(),
			handler: func(attempts *atomic.Int32, processed chan<- struct{}) kafkaclient.MessagesHandler[string] {
				return func(messages []*kafkaclient.MessageWithContext[string]) error {
					attempts.Add(1)
					processed <- struct{}{}
					return errors.New("permanent failure")
				}
			},
			messageCount: 6,
		},
	}

	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			topic := createTestTopic(t, s.admin, tt.topic)
			var attempts atomic.Int32
			processed := make(chan struct{}, 1)

			consumer := s.newRetryBatchConsumer(topic, tt.handler(&attempts, processed), tt.retryConfig)

			s.runRetryTest(t, retryTestConfig{
				name:             tt.name,
				topic:            topic,
				expectedAttempts: tt.expectedAttempts,
				retryConfig:      tt.retryConfig,
				setupMessages:    func() { s.produceMessages(topic, tt.messageCount) },
				consumer:         consumer,
				attempts:         &attempts,
				processed:        processed,
			})
		})
	}
}
