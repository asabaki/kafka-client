package kafkaclient

import (
	"context"
	"time"

	"github.com/IBM/sarama"
)

type producerHeartbeat struct {
	ctxCancelFunc func()
	ticker        *time.Ticker
	client        sarama.Client
	logger        Logger
}

func newProducerHeartbeat(config KafkaConfig, client sarama.Client) producerHeartbeat {
	return producerHeartbeat{
		client: client,
		logger: config.log(),
		ticker: time.NewTicker(config.ProducerHeartbeatInterval),
	}
}

func (p *producerHeartbeat) startProcess() {
	ctx := context.Background()
	ctxWithCancel, cancel := context.WithCancel(ctx)
	p.ctxCancelFunc = cancel
	go func() {
		defer p.ticker.Stop()
		for {
			if p.ticker == nil {
				p.logger.Error(ctxWithCancel, "producer heartbeat ticker stopped due to empty ticker")
				break
			}

			select {
			case <-ctxWithCancel.Done():
				p.logger.Info(context.Background(), "producer heartbeat stopped")
				return
			case <-p.ticker.C:
				p.logger.Debug(ctxWithCancel, "producer heartbeat ticked")
				p.processHeartbeatEvent()
			}
		}
	}()
}

func (p *producerHeartbeat) processHeartbeatEvent() {
	ctx := context.Background()
	for _, one := range p.client.Brokers() {
		connected, connectionErr := one.Connected()
		if connectionErr != nil || !connected {
			p.logger.Warn(ctx, "producer heartbeat: skip broker %s that failed to connect: %v", one.Addr(), connectionErr)
			continue
		}

		_, err := one.ApiVersions(&sarama.ApiVersionsRequest{})
		if err != nil {
			p.logger.Warn(ctx, "producer heartbeat: failed to send request to broker %s: %s", one.Addr(), err)
			continue
		}
		p.logger.Debug(ctx, "producer heartbeat: sent request to broker %s", one.Addr())
	}
}

func (p *producerHeartbeat) stop() {
	if p.ticker != nil {
		p.ticker.Stop()
	}
	if p.ctxCancelFunc != nil {
		p.ctxCancelFunc()
	}
}
