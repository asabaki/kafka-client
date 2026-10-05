package kafkaclient

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_calculateBackoff(t *testing.T) {
	tests := []struct {
		name       string
		retryCount int
		baseDelay  time.Duration
		maxDelay   time.Duration
		want       time.Duration
	}{
		{
			name:       "first retry",
			retryCount: 0,
			baseDelay:  100 * time.Millisecond,
			maxDelay:   30 * time.Second,
			want:       100 * time.Millisecond,
		},
		{
			name:       "second retry",
			retryCount: 1,
			baseDelay:  100 * time.Millisecond,
			maxDelay:   30 * time.Second,
			want:       200 * time.Millisecond,
		},
		{
			name:       "third retry",
			retryCount: 2,
			baseDelay:  1 * time.Second,
			maxDelay:   30 * time.Second,
			want:       4 * time.Second,
		},
		{
			name:       "negative retry count should be treated as zero",
			retryCount: -1,
			baseDelay:  100 * time.Millisecond,
			maxDelay:   30 * time.Second,
			want:       100 * time.Millisecond,
		},
		{
			name:       "should respect max delay",
			retryCount: 10,
			baseDelay:  100 * time.Millisecond,
			maxDelay:   1 * time.Second,
			want:       1 * time.Second,
		},
		{
			name:       "zero base delay",
			retryCount: 5,
			baseDelay:  0,
			maxDelay:   30 * time.Second,
			want:       0,
		},
		{
			name:       "zero max delay",
			retryCount: 5,
			baseDelay:  100 * time.Millisecond,
			maxDelay:   0,
			want:       0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateBackoff(tt.retryCount, tt.baseDelay, tt.maxDelay)
			assert.Equal(t, tt.want, got)
		})
	}
}
