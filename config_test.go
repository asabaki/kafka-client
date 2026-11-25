package kafkaclient

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestToSaramaConfig_IsolationLevel(t *testing.T) {
	tests := []struct {
		name string
		give KafkaConfig
		want sarama.IsolationLevel
	}{
		{
			name: "should default ReadCommitted IsolationLevel",
			give: KafkaConfig{},
			want: sarama.ReadCommitted,
		},
		{
			name: "should set ReadCommitted IsolationLevel",
			give: KafkaConfig{IsolationLevel: IsolationLevelReadCommitted},
			want: sarama.ReadCommitted,
		},
		{
			name: "should set ReadUncommitted IsolationLevel",
			give: KafkaConfig{IsolationLevel: IsolationLevelReadUncommitted},
			want: sarama.ReadUncommitted,
		},
		{
			name: "should default ReadCommitted IsolationLevel if set invalid isolation level",
			give: KafkaConfig{IsolationLevel: IsolationLevel("invalid")},
			want: sarama.ReadCommitted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := tt.give.ToSaramaConfig()
			assert.Equal(t, tt.want, conf.Consumer.IsolationLevel)
		})
	}
}

func TestToSaramaConfig_TLSConfigServerName(t *testing.T) {
	tests := []struct {
		name string
		give KafkaConfig
		opts []KafkaConfigOption
		want string
	}{
		{
			name: "should set ServerName when TLSConfigServerName is provided",
			give: KafkaConfig{
				TlsEnabled:          true,
				TLSConfigServerName: "example.com",
			},
			want: "example.com",
		},
		{
			name: "should not set ServerName when TLSConfigServerName is empty",
			give: KafkaConfig{
				TlsEnabled:          true,
				TLSConfigServerName: "",
			},
			want: "",
		},
		{
			name: "should not set ServerName when TLS is disabled even if TLSConfigServerName is provided",
			give: KafkaConfig{
				TlsEnabled:          false,
				TLSConfigServerName: "example.com",
			},
			want: "",
		},
		{
			name: "should not set ServerName when WithTLSConfig is used even if TLSConfigServerName is provided",
			give: KafkaConfig{
				TlsEnabled:          true,
				TLSConfigServerName: "custom.example.com",
			},
			opts: []KafkaConfigOption{
				WithTLSConfig(&tls.Config{
					InsecureSkipVerify: true, //nolint:gosec
				}),
			},
			want: "",
		},
		{
			name: "should not override existing ServerName in WithTLSConfig when TLSConfigServerName is empty",
			give: KafkaConfig{
				TlsEnabled:          true,
				TLSConfigServerName: "",
			},
			opts: []KafkaConfigOption{
				WithTLSConfig(&tls.Config{ //nolint:gosec
					ServerName: "preset.example.com",
				}),
			},
			want: "preset.example.com",
		},
		{
			name: "should not override existing ServerName in WithTLSConfig even when TLSConfigServerName is provided",
			give: KafkaConfig{
				TlsEnabled:          true,
				TLSConfigServerName: "override.example.com",
			},
			opts: []KafkaConfigOption{
				WithTLSConfig(&tls.Config{ //nolint:gosec
					ServerName: "preset.example.com",
				}),
			},
			want: "preset.example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := tt.give.ToSaramaConfig(tt.opts...)
			if tt.give.TlsEnabled {
				assert.NotNil(t, conf.Net.TLS.Config)
				assert.Equal(t, tt.want, conf.Net.TLS.Config.ServerName)
			} else {
				assert.Nil(t, conf.Net.TLS.Config)
			}
		})
	}
}

func TestToSaramaConfig_NetTimeouts(t *testing.T) {
	def := KafkaConfig{}.ToSaramaConfig()
	assert.Equal(t, 30*time.Second, def.Net.DialTimeout, "0 keeps sarama's default")
	assert.Equal(t, 30*time.Second, def.Net.ReadTimeout)
	assert.Equal(t, 30*time.Second, def.Net.WriteTimeout)

	c := KafkaConfig{NetDialTimeout: time.Second, NetReadTimeout: 2 * time.Second, NetWriteTimeout: 3 * time.Second}.ToSaramaConfig()
	assert.Equal(t, time.Second, c.Net.DialTimeout)
	assert.Equal(t, 2*time.Second, c.Net.ReadTimeout)
	assert.Equal(t, 3*time.Second, c.Net.WriteTimeout)
}

func TestToSaramaConfig_RetryBuffer(t *testing.T) {
	def := KafkaConfig{}.ToSaramaConfig()
	assert.Equal(t, 10_000, def.Producer.Retry.MaxBufferLength, "capped by default")
	assert.Equal(t, int64(64<<20), def.Producer.Retry.MaxBufferBytes)

	unlimited := KafkaConfig{ProducerRetryMaxBufferLength: -1, ProducerRetryMaxBufferBytes: -1}.ToSaramaConfig()
	assert.Equal(t, -1, unlimited.Producer.Retry.MaxBufferLength)
	assert.Equal(t, int64(-1), unlimited.Producer.Retry.MaxBufferBytes)

	custom := KafkaConfig{ProducerRetryMaxBufferLength: 5000, ProducerRetryMaxBufferBytes: 40 << 20}.ToSaramaConfig()
	assert.Equal(t, 5000, custom.Producer.Retry.MaxBufferLength)
	assert.Equal(t, int64(40<<20), custom.Producer.Retry.MaxBufferBytes)
}

func TestToSaramaConfig_BalanceStrategies(t *testing.T) {
	names := func(c *sarama.Config) []string {
		var out []string
		for _, s := range c.Consumer.Group.Rebalance.GroupStrategies {
			out = append(out, s.Name())
		}
		return out
	}

	assert.Equal(t, []string{"range"}, names(KafkaConfig{}.ToSaramaConfig()), "empty keeps range")

	rollout := KafkaConfig{ConsumerBalanceStrategies: []string{BalanceStrategyCooperativeSticky, BalanceStrategyRange}}
	assert.Equal(t, []string{"cooperative-sticky", "range"}, names(rollout.ToSaramaConfig()))

	a, b := rollout.ToSaramaConfig(), rollout.ToSaramaConfig()
	assert.NotSame(t, a.Consumer.Group.Rebalance.GroupStrategies[0], b.Consumer.Group.Rebalance.GroupStrategies[0],
		"cooperative-sticky is stateful: each consumer group needs its own")

	cooperative := KafkaConfig{ProducerTimeout: time.Second, ConsumerBalanceStrategies: []string{BalanceStrategyCooperativeSticky}}.ToSaramaConfig()
	cooperative.Version = sarama.V2_4_0_0
	assert.NoError(t, cooperative.Validate())
}

// A typo in the shared connection config must be reported as itself by every client built from it: sarama validates
// the consumer group settings for producers too, and would otherwise fail with an unrelated message.
func TestConstructors_RejectUnknownBalanceStrategy(t *testing.T) {
	cfg := KafkaConfig{BootstrapServers: []string{"localhost:1"}, ConsumerBalanceStrategies: []string{"cooperative_sticky"}}
	const want = `unknown consumer balance strategies ["cooperative_sticky"]`

	_, err := NewConsumerGroup(cfg, "group", "topic", false, func(context.Context, *sarama.ConsumerMessage) error { return nil })
	assert.ErrorContains(t, err, want, "consumer group")
	_, err = NewBatchConsumerGroup(cfg, "group", "topic", 10, time.Second, false,
		func([]*MessageWithContext[*sarama.ConsumerMessage]) error { return nil })
	assert.ErrorContains(t, err, want, "batch consumer group")
	_, err = NewSyncProducer(cfg)
	assert.ErrorContains(t, err, want, "sync producer")
	_, err = NewAsyncProducer(cfg)
	assert.ErrorContains(t, err, want, "async producer")
}

func TestToSaramaConfig_GroupTimeouts(t *testing.T) {
	def := KafkaConfig{}.ToSaramaConfig()
	assert.Equal(t, 10*time.Second, def.Consumer.Group.Session.Timeout, "0 keeps sarama's default")
	assert.Equal(t, 3*time.Second, def.Consumer.Group.Heartbeat.Interval)
	assert.Equal(t, 60*time.Second, def.Consumer.Group.Rebalance.Timeout)

	c := KafkaConfig{
		ConsumerSessionTimeout:    45 * time.Second,
		ConsumerHeartbeatInterval: 5 * time.Second,
		ConsumerRebalanceTimeout:  20 * time.Second,
	}.ToSaramaConfig()
	assert.Equal(t, 45*time.Second, c.Consumer.Group.Session.Timeout)
	assert.Equal(t, 5*time.Second, c.Consumer.Group.Heartbeat.Interval)
	assert.Equal(t, 20*time.Second, c.Consumer.Group.Rebalance.Timeout)
}
