// gosoline-service is a gosoline app with a single-message consumer, a batch consumer, a sync producer shared
// between the consumer and an HTTP endpoint, and an async producer, all configured in config.dist.yml.
//
//	cd gosoline-service && KAFKA_BOOTSTRAP_SERVERS=localhost:9092 go run .
//	curl -XPOST 'localhost:8088/orders?id=order-1' -d '{"id":"order-1"}'
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/justtrackio/gosoline/pkg/application"
	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/httpserver"
	"github.com/justtrackio/gosoline/pkg/kernel"
	"github.com/justtrackio/gosoline/pkg/log"

	kafkaclient "github.com/asabaki/kafka-client"
	"github.com/asabaki/kafka-client/kafkagoso"
)

func main() {
	application.Run(
		application.WithConfigFile("config.dist.yml", "yml"),
		application.WithTracing, // trace_id in logs, gosoline tracer for consumer spans
		application.WithModuleMultiFactory(modules),
		// Always register the closer when using Provide*Producer: it flushes and closes them on shutdown,
		// after the consumers and the HTTP server (application stage) have stopped.
		application.WithModuleFactory("kafka_producer_closer", kafkagoso.NewProducerCloserModuleFactory()),
	)
}

// modules builds everything from one factory, so the producers are created once and shared.
func modules(ctx context.Context, config cfg.Config, logger log.Logger) (map[string]kernel.ModuleFactory, error) {
	events, err := kafkagoso.ProvideSyncProducer(ctx, config, logger, "order_events")
	if err != nil {
		return nil, err
	}
	audit, err := kafkagoso.ProvideAsyncProducer(ctx, config, logger, "audit_log")
	if err != nil {
		return nil, err
	}

	return map[string]kernel.ModuleFactory{
		"orders_consumer":  kafkagoso.NewConsumerModuleFactory("orders", orderHandler(events, audit)),
		"archive_consumer": kafkagoso.NewBatchConsumerModuleFactory("order_events_archive", archiveHandler),
		"api":              httpserver.New("default", router(events)),
	}, nil
}

func orderHandler(events kafkaclient.SyncProducer, audit kafkaclient.AsyncProducer) kafkagoso.HandlerFactory {
	return func(_ context.Context, _ cfg.Config, logger log.Logger) (kafkaclient.MessageHandler[*sarama.ConsumerMessage], error) {
		return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			logger.Info(ctx, "accepting order %s", msg.Key) // carries trace_id from the message headers

			// Sync: an error redelivers the order, so the event can't be lost.
			if _, _, err := events.PublishRawAtLeastOnce(ctx, "order-events", string(msg.Key), []byte(`{"status":"accepted"}`), nil); err != nil {
				return fmt.Errorf("publish order event: %w", err)
			}
			// Async: best effort, flushed on shutdown by the closer module.
			audit.PublishRawAtMostOnce(ctx, "audit-log", string(msg.Key), msg.Value, map[string]string{"action": "order-accepted"})

			return nil
		}, nil
	}
}

func archiveHandler(_ context.Context, _ cfg.Config, logger log.Logger) (kafkaclient.MessagesHandler[*sarama.ConsumerMessage], error) {
	return func(messages []*kafkaclient.MessageWithContext[*sarama.ConsumerMessage]) error {
		ctx, first := messages[0].Get()
		logger.Info(ctx, "archiving %d order events starting at offset %d", len(messages), first.Offset)
		return nil
	}, nil
}

// router exposes POST /orders, which publishes straight to Kafka with the same producer the consumer uses.
func router(events kafkaclient.SyncProducer) httpserver.Definer {
	return func(_ context.Context, _ cfg.Config, _ log.Logger) (*httpserver.Definitions, error) {
		d := &httpserver.Definitions{}
		d.POST("/orders", func(c *gin.Context) {
			body, err := io.ReadAll(c.Request.Body)
			if err != nil {
				c.String(http.StatusBadRequest, err.Error())
				return
			}
			// Bound the publish: the request fails fast instead of waiting out a broker outage.
			ctx, cancel := context.WithTimeout(c.Request.Context(), time.Second)
			defer cancel()

			partition, offset, err := events.PublishRawAtLeastOnce(ctx, "orders", c.Query("id"), body, nil)
			if err != nil {
				c.String(http.StatusServiceUnavailable, err.Error())
				return
			}
			c.JSON(http.StatusOK, gin.H{"partition": partition, "offset": offset})
		})
		return d, nil
	}
}
