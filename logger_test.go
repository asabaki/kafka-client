package kafkaclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (r *recordingLogger) add(level, format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, level+" "+fmt.Sprintf(format, args...))
}

func (r *recordingLogger) Debug(_ context.Context, f string, a ...any) { r.add("DEBUG", f, a...) }
func (r *recordingLogger) Info(_ context.Context, f string, a ...any)  { r.add("INFO", f, a...) }
func (r *recordingLogger) Warn(_ context.Context, f string, a ...any)  { r.add("WARN", f, a...) }
func (r *recordingLogger) Error(_ context.Context, f string, a ...any) { r.add("ERROR", f, a...) }

func TestRetryLogsOnlyWhenLoggerIsSet(t *testing.T) {
	failing := func() error { return errors.New("boom") }

	// no logger: must not panic, nothing to observe
	assert.NoError(t, RetryWithBackoff(RetryConfig{MaxRetries: 1}, failing))

	logger := &recordingLogger{}
	assert.NoError(t, RetryWithBackoff(RetryConfig{MaxRetries: 1, Logger: logger}, failing))
	assert.Equal(t, []string{
		"INFO retrying message handling (attempt 1) after 0s: boom",
		"WARN message exceeded max retries (1), marking as processed: boom",
	}, logger.lines)
}

func TestWithLoggerIsAppliedToConfig(t *testing.T) {
	assert.Equal(t, nopLogger{}, KafkaConfig{}.log())

	logger := &recordingLogger{}
	assert.Same(t, logger, KafkaConfig{}.withOptions(WithLogger(logger)).log())
}

func TestAsyncProducerDefaultErrorHandlerUsesLogger(t *testing.T) {
	logger := &recordingLogger{}
	p := &asyncProducer{logger: logger}

	p.logProducerError(&sarama.ProducerError{Msg: &sarama.ProducerMessage{Topic: "t", Key: sarama.StringEncoder("k")}, Err: errors.New("boom")})
	p.logProducerError(&sarama.ProducerError{Msg: &sarama.ProducerMessage{Topic: "t"}, Err: errors.New("boom")})

	assert.Equal(t, []string{
		"ERROR produce message error: topic=t key=k: boom",
		"ERROR produce message error: topic=t: boom",
	}, logger.lines)
}

func TestSaramaLoggerForwardsAtDebug(t *testing.T) {
	logger := &recordingLogger{}
	l := saramaLogger{logger: logger}

	l.Printf("connected to %s\n", "broker-1")
	l.Println("closing")
	l.Print("a", "b")

	assert.Equal(t, []string{
		"DEBUG [sarama] connected to broker-1",
		"DEBUG [sarama] closing",
		"DEBUG [sarama] ab",
	}, logger.lines)
}

func TestPrepareConfigInstallsSaramaLoggerOnlyWithDebugAndLogger(t *testing.T) {
	prev := sarama.Logger
	t.Cleanup(func() { sarama.Logger = prev })

	prepareConfig(KafkaConfig{Debug: true})
	assert.Equal(t, prev, sarama.Logger, "debug without logger must not touch sarama.Logger")

	prepareConfig(KafkaConfig{}, WithLogger(&recordingLogger{}))
	assert.Equal(t, prev, sarama.Logger, "logger without debug must not touch sarama.Logger")

	logger := &recordingLogger{}
	prepareConfig(KafkaConfig{Debug: true}, WithLogger(logger))
	assert.Equal(t, saramaLogger{logger: logger}, sarama.Logger)
}
