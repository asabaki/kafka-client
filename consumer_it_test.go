package kafkaclient_test

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/asabaki/kafka-client"
)

func TestConsumerITSuite(t *testing.T) {
	requireKafka(t)

	kafkaCfg := makeKafkaConfigForTest()

	suite.Run(t, &ConsumerITSuite{
		kafkaCfg: kafkaCfg,
		producer: createProducerForTest(t, kafkaCfg.BootstrapServers),
		admin:    createClusterAdmin(t, kafkaCfg.BootstrapServers),
	})
}

type ConsumerITSuite struct {
	suite.Suite
	kafkaCfg kafkaclient.KafkaConfig
	admin    sarama.ClusterAdmin
	producer sarama.SyncProducer
}

func (s *ConsumerITSuite) produceMessage(topic string, str string) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(str),
	}
	_, _, err := s.producer.SendMessage(msg)
	s.Require().NoError(err)
	log.Printf("publish   message: %#v", msg)
}

func (s *ConsumerITSuite) newConsumer(
	topic string,
	handler kafkaclient.MessageHandler[string]) kafkaclient.ConsumerGroup {
	groupID := fmt.Sprintf("%s-%s", topic, getNextSequence())
	consumer, err := kafkaclient.NewConsumerGroup(s.kafkaCfg, groupID, topic, false, kafkaclient.WrapWithSaramaMessageHandler(handler, simpleStringConverter))
	s.Require().NoError(err)
	log.Printf("consumer group created: group_id=%s topic=%s", groupID, topic)
	return consumer
}

func (s *ConsumerITSuite) TestSimple() {
	topic := createTestTopic(s.T(), s.admin, "test-simple")

	s.T().Run("consume", func(t *testing.T) {
		expectedMsg := fmt.Sprintf("message-%s", time.Now())
		s.produceMessage(topic, expectedMsg)

		processed := make(chan string, 1)
		consumer := s.newConsumer(topic, func(ctx context.Context, msg string) error {
			processed <- msg
			return nil
		})
		defer consumer.Close()

		assert.False(t, consumer.IsRunning())
		assert.Error(t, consumer.Health())

		err := consumer.Start()
		assert.NoError(t, err)

		assert.Eventually(t, consumer.IsRunning, 10*time.Second, 50*time.Millisecond)

		assert.NoError(t, consumer.Health())

		select {
		case msg := <-processed:
			assert.Equal(t, msg, expectedMsg)
		case <-time.After(15 * time.Second):
			assert.Fail(t, "Timed out waiting for processing")
		}
	})

	for i := 0; i < 10; i++ {
		s.T().Run("concurrent "+strconv.Itoa(i), func(t *testing.T) {
			consumer := s.newConsumer(topic, func(ctx context.Context, msg string) error {
				return nil
			})
			defer consumer.Close()
			err := consumer.Start()
			assert.NoError(t, err)
		})
	}
}

func (s *ConsumerITSuite) TestCloseWithoutStart() {
	topic := createTestTopic(s.T(), s.admin, "test-close-without-start")
	consumer := s.newConsumer(topic, func(ctx context.Context, msg string) error {
		return nil
	})

	consumer.Close()
}

var simpleStringConverter = kafkaclient.ConsumerMessageConverterFunc[string](func(msg *sarama.ConsumerMessage) (string, error) {
	return string(msg.Value), nil
})
