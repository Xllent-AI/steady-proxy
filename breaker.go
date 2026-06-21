package main

import (
	"sync"
	"time"
)

// ---------------------------------------------------------- circuit breaker --
// Keyed by failure domain (upstream host | model), NOT by request body. Bounds
// the blast radius of a real upstream outage so a wave of subagents can't
// hammer a dead gateway.

const (
	cbConsecToOpen = 8
	cbWindow       = 30 * time.Second
	cbMinSamples   = 20
	cbInitialOpen  = 15 * time.Second
	cbMaxOpen      = 2 * time.Minute
)

type cbState struct {
	consecFail  int
	winStart    time.Time
	failures    int
	total       int
	openUntil   time.Time
	openDur     time.Duration
	halfProbing bool
}

type circuitBreaker struct {
	mu sync.Mutex
	m  map[string]*cbState
}

func newCircuitBreaker() *circuitBreaker { return &circuitBreaker{m: map[string]*cbState{}} }

func (c *circuitBreaker) get(route string) *cbState {
	s := c.m[route]
	if s == nil {
		s = &cbState{winStart: time.Now()}
		c.m[route] = s
	}
	return s
}

// isOpen reports whether the circuit is open (and the remaining cool-down).
// When the cool-down has elapsed it allows a single half-open probe through.
func (c *circuitBreaker) isOpen(route string) (time.Duration, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.get(route)
	now := time.Now()
	if s.openUntil.IsZero() {
		return 0, false
	}
	if now.Before(s.openUntil) {
		return s.openUntil.Sub(now), true
	}
	// cool-down elapsed → let exactly ONE probe through; hold others until it resolves
	if s.halfProbing {
		return time.Second, true
	}
	s.halfProbing = true
	return 0, false
}

func (c *circuitBreaker) recordSuccess(route string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.get(route)
	s.consecFail = 0
	s.failures = 0
	s.total = 0
	s.winStart = time.Now()
	s.openUntil = time.Time{}
	s.openDur = 0
	s.halfProbing = false
}

func (c *circuitBreaker) recordFailure(route string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.get(route)
	now := time.Now()

	if s.halfProbing { // probe failed → reopen, longer
		s.halfProbing = false
		s.openDur = min(cbMaxOpen, maxDur(cbInitialOpen, s.openDur*2))
		s.openUntil = now.Add(s.openDur)
		return
	}

	if now.Sub(s.winStart) > cbWindow {
		s.winStart = now
		s.failures = 0
		s.total = 0
	}
	s.consecFail++
	s.failures++
	s.total++

	rateTrip := s.total >= cbMinSamples && s.failures*2 >= s.total
	if s.consecFail >= cbConsecToOpen || rateTrip {
		s.openDur = maxDur(cbInitialOpen, s.openDur)
		s.openUntil = now.Add(s.openDur)
	}
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// ----------------------------------------------------------- episode ledger --
// Fallback counter for the rare case where Claude Code starts a brand-new SDK
// request (resetting X-Stainless-Retry-Count) for what is logically the same
// failing call. Keyed by the request fingerprint; stores only counters.

type episode struct {
	first, last    time.Time
	convertedCount int
	sameFaultCount int
	lastCode       string
}

type episodeLedger struct {
	mu  sync.Mutex
	m   map[string]*episode
	ttl time.Duration
}

func newEpisodeLedger(ttl time.Duration) *episodeLedger {
	return &episodeLedger{m: map[string]*episode{}, ttl: ttl}
}

// bump records one converted failure for key and returns a snapshot.
func (l *episodeLedger) bump(key, code string) episode {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	for k, e := range l.m {
		if now.Sub(e.last) > l.ttl {
			delete(l.m, k)
		}
	}
	e := l.m[key]
	if e == nil {
		e = &episode{first: now}
		l.m[key] = e
	}
	e.convertedCount++
	if code == e.lastCode {
		e.sameFaultCount++
	} else {
		e.sameFaultCount = 1
		e.lastCode = code
	}
	e.last = now
	return *e
}

func (l *episodeLedger) clear(key string) {
	l.mu.Lock()
	delete(l.m, key)
	l.mu.Unlock()
}
