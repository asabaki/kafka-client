package timeutil_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/asabaki/kafka-client/internal/timeutil"
)

func TestRealClockByDefault(t *testing.T) {
	assert.False(t, timeutil.IsFrozen())

	before := time.Now()
	got := timeutil.Now()
	assert.False(t, got.Before(before))
	assert.WithinDuration(t, time.Now(), got, time.Second)
}

func TestFreezeAdvanceUnfreeze(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	t.Run("frozen", func(t *testing.T) {
		timeutil.Freeze(t, at)
		assert.True(t, timeutil.IsFrozen())
		assert.Equal(t, at, timeutil.Now())
		assert.Equal(t, at, timeutil.Now(), "does not move on its own")

		timeutil.Advance(90 * time.Second)
		assert.Equal(t, at.Add(90*time.Second), timeutil.Now())
		assert.Equal(t, 90*time.Second, timeutil.Since(at))
	})

	assert.False(t, timeutil.IsFrozen(), "Freeze(t, ...) unfreezes when the test ends")
}

func TestAdvancePanicsWhenNotFrozen(t *testing.T) {
	assert.PanicsWithValue(t, "timeutil.Advance: clock is not frozen", func() { timeutil.Advance(time.Second) })
}

func TestFreezeWithoutCleanup(t *testing.T) {
	timeutil.Freeze(nil, time.Unix(0, 0))
	assert.True(t, timeutil.IsFrozen())

	timeutil.Unfreeze()
	assert.False(t, timeutil.IsFrozen())
}
