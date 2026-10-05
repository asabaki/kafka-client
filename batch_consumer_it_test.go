package kafkaclient_test

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/asabaki/kafka-client"
)

func TestBatchConsumerITSuite(t *testing.T) {
	requireKafka(t)

	kafkaCfg := makeKafkaConfigForTest()

	suite.Run(t, &BatchConsumerITSuite{
		kafkaCfg:     kafkaCfg,
		producer:     createProducerForTest(t, kafkaCfg.BootstrapServers),
		admin:        createClusterAdmin(t, kafkaCfg.BootstrapServers),
		batchSize:    3,
		batchTimeout: 2 * time.Second,
	})
}

type BatchConsumerITSuite struct {
	suite.Suite
	kafkaCfg     kafkaclient.KafkaConfig
	producer     sarama.SyncProducer
	admin        sarama.ClusterAdmin
	batchSize    uint16
	batchTimeout time.Duration
}

func (s *BatchConsumerITSuite) produceMessages(topic string, n uint16) {
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

func (s *BatchConsumerITSuite) newConsumer(topic string, handler kafkaclient.MessagesHandler[string]) kafkaclient.ConsumerGroup {
	groupID := fmt.Sprintf("%s-%s", topic, getNextSequence())
	consumer, err := kafkaclient.NewBatchConsumerGroup(s.kafkaCfg, groupID, topic, s.batchSize, s.batchTimeout, false, kafkaclient.WrapWithSaramaMessagesHandler(handler, simpleStringConverter))
	s.Require().NoError(err)
	log.Printf("consumer group created: group_id=%s topic=%s batch_size=%d batch_timeout=%s", groupID, topic, s.batchSize, s.batchTimeout)
	return consumer
}

func (s *BatchConsumerITSuite) Test_BatchTriggered_ByBatchSize() {
	topic := createTestTopic(s.T(), s.admin, "test-batch")

	s.T().Run("batch", func(t *testing.T) {
		s.produceMessages(topic, s.batchSize)

		batchProcessed := make(chan int, 1)
		consumer := s.newConsumer(topic, func(messages []*kafkaclient.MessageWithContext[string]) error {
			batchProcessed <- len(messages)
			return nil
		})
		defer consumer.Close()

		err := consumer.Start()
		require.NoError(t, err)

		assert.Eventually(t, consumer.IsRunning, 10*time.Second, 50*time.Millisecond)

		select {
		case count := <-batchProcessed:
			assert.Equal(t, int(s.batchSize), count)
		case <-time.After(15 * time.Second):
			assert.Fail(t, "Timed out waiting for batch processing")
		}
	})

	for i := 0; i < 10; i++ {
		s.T().Run("concurrent "+strconv.Itoa(i), func(t *testing.T) {
			consumer := s.newConsumer(topic, func(messages []*kafkaclient.MessageWithContext[string]) error {
				return nil
			})
			defer consumer.Close()

			err := consumer.Start()
			require.NoError(t, err)
		})
	}
}

func (s *BatchConsumerITSuite) Test_BatchTriggered_ByTimeout() {
	topic := createTestTopic(s.T(), s.admin, "test-batch")
	s.produceMessages(topic, 1)

	batchProcessed := make(chan int, 1)

	consumer := s.newConsumer(topic, func(messages []*kafkaclient.MessageWithContext[string]) error {
		batchProcessed <- len(messages)
		return nil
	})
	defer consumer.Close()

	err := consumer.Start()
	require.NoError(s.T(), err)

	select {
	case count := <-batchProcessed:
		s.Equal(1, count)
	case <-time.After(s.batchTimeout + 15*time.Second):
		s.Fail("Timed out waiting for batch processing")
	}
}
