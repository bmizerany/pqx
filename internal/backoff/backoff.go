// This was copied from tailscale.com/logtail/backoff and modified only enough
// to break the dependencies on tailscale.

// Copyright (c) 2020 Tailscale Inc & AUTHORS All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package backoff provides a back-off timer type.
package backoff

import (
	"context"
	"math/rand"
	"time"
)

// Backoff tracks state the history of consecutive failures and sleeps
// an increasing amount of time, up to a provided limit.
type Backoff struct {
	n          int // number of consecutive failures
	maxBackoff time.Duration

	// Name is the name of this backoff timer, for logging purposes.
	name string
	// logf is the function used for log messages when backing off.
	logf func(format string, args ...any)

	// NewTimer is the function that acts like time.NewTimer.
	// It's for use in unit tests.
	NewTimer func(time.Duration) *time.Timer

	// LogLongerThan sets the minimum time of a single backoff interval
	// before we mention it in the log.
	LogLongerThan time.Duration
}

// NewBackoff returns a new Backoff timer with the provided name (for logging), logger,
// and max backoff time. By default, all failures (calls to BackOff with a non-nil err)
// are logged unless the returned Backoff.LogLongerThan is adjusted.
func NewBackoff(name string, logf func(string, ...any), maxBackoff time.Duration) *Backoff {
	return &Backoff{
		name:       name,
		logf:       logf,
		maxBackoff: maxBackoff,
		NewTimer:   time.NewTimer,
	}
}

// Backoff sleeps an increasing amount of time if err is non-nil.
// and the context is not a
// It resets the backoff schedule once err is nil.
func (b *Backoff) BackOff(ctx context.Context, err error) {
	if err == nil {
		// No error. Reset number of consecutive failures.
		b.n = 0
		return
	}
	if ctx.Err() != nil {
		// Fast path.
		return
	}

	b.n++
	// n^2 backoff timer is a little smoother than the
	// common choice of 2^n.
	d := time.Duration(b.n*b.n) * 10 * time.Millisecond
	if d > b.maxBackoff {
		d = b.maxBackoff
	}
	// Randomize the delay between 0.5-1.5 x msec, in order
	// to prevent accidental "thundering herd" problems.
	d = time.Duration(float64(d) * (rand.Float64() + 0.5))

	if d >= b.LogLongerThan {
		b.logf("%s: [v1] backoff: %d msec", b.name, d.Milliseconds())
	}
	t := b.NewTimer(d)
	select {
	case <-ctx.Done():
		t.Stop()
	case <-t.C:
	}
}
