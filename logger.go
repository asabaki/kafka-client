package kafkaclient

import (
	"context"
	"fmt"
	"strings"
)

// Logger is the logging interface used by kafka-client.
//
// Logging is opt-in: nothing is logged unless a Logger is supplied via WithLogger (or RetryConfig.Logger).
// The interface is satisfied as-is by gosoline's log.Logger.
type Logger interface {
	Debug(ctx context.Context, format string, args ...any)
	Info(ctx context.Context, format string, args ...any)
	Warn(ctx context.Context, format string, args ...any)
	Error(ctx context.Context, format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Debug(context.Context, string, ...any) {}
func (nopLogger) Info(context.Context, string, ...any)  {}
func (nopLogger) Warn(context.Context, string, ...any)  {}
func (nopLogger) Error(context.Context, string, ...any) {}

func loggerOrNop(l Logger) Logger {
	if l == nil {
		return nopLogger{}
	}
	return l
}

// saramaLogger adapts Logger to sarama.StdLogger, logging at debug level.
type saramaLogger struct {
	logger Logger
}

func (s saramaLogger) Print(v ...any) {
	s.logger.Debug(context.Background(), "[sarama] %s", fmt.Sprint(v...))
}

func (s saramaLogger) Printf(format string, v ...any) {
	s.logger.Debug(context.Background(), "[sarama] %s", strings.TrimSuffix(fmt.Sprintf(format, v...), "\n"))
}

func (s saramaLogger) Println(v ...any) {
	s.logger.Debug(context.Background(), "[sarama] %s", strings.TrimSuffix(fmt.Sprintln(v...), "\n"))
}
