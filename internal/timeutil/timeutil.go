// Package timeutil is the library's clock. Code reads the current time through Now and Since, so tests can pin
// the time with Freeze and move it with Advance instead of sleeping.
//
// The clock is process-wide. Tests that freeze it must not run in parallel with other tests that read it, and
// must restore it with Unfreeze (Freeze registers that automatically when given a testing.TB).
package timeutil

import (
	"sync"
	"time"
)

var (
	mu     sync.RWMutex
	frozen *time.Time
)

// Now returns the frozen time if the clock is frozen, otherwise time.Now().
func Now() time.Time {
	mu.RLock()
	defer mu.RUnlock()

	if frozen != nil {
		return *frozen
	}
	return time.Now()
}

// Since returns the time elapsed since t according to Now.
func Since(t time.Time) time.Duration {
	return Now().Sub(t)
}

// Cleanuper is the part of testing.TB that Freeze needs.
type Cleanuper interface {
	Cleanup(func())
}

// Freeze stops the clock at t. When tb is non-nil, the clock is unfrozen when the test ends.
func Freeze(tb Cleanuper, t time.Time) {
	mu.Lock()
	frozen = &t
	mu.Unlock()

	if tb != nil {
		tb.Cleanup(Unfreeze)
	}
}

// Advance moves a frozen clock forward by d. It panics if the clock is not frozen, since moving the real clock
// is impossible and a silent no-op would hide a broken test.
func Advance(d time.Duration) {
	mu.Lock()
	defer mu.Unlock()

	if frozen == nil {
		panic("timeutil.Advance: clock is not frozen")
	}
	t := frozen.Add(d)
	frozen = &t
}

// Unfreeze returns the clock to real time.
func Unfreeze() {
	mu.Lock()
	frozen = nil
	mu.Unlock()
}

// IsFrozen reports whether the clock is frozen.
func IsFrozen() bool {
	mu.RLock()
	defer mu.RUnlock()

	return frozen != nil
}
