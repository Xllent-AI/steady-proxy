package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

func stopRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

func bufferDeadline(start time.Time, window time.Duration) time.Time {
	if window <= 0 {
		return time.Time{}
	}
	return start.Add(window)
}

// Until SSE capture takes ownership, headers and error-body reads share the
// request's absolute no-output deadline. Closing the body always releases the
// attempt context; stopping the deadline alone lets a healthy stream go live.
func roundTripBefore(ctx context.Context, r *http.Request, body []byte, deadline time.Time) (*http.Response, time.Time, func(), error) {
	attemptCtx, cancel := context.WithCancelCause(ctx)
	stop := func() {}
	if !deadline.IsZero() {
		if time.Until(deadline) <= 0 {
			cancel(context.DeadlineExceeded)
		} else {
			timer := time.AfterFunc(time.Until(deadline), func() { cancel(context.DeadlineExceeded) })
			stop = func() { timer.Stop() }
		}
	}
	resp, started, err := roundTrip(attemptCtx, r, body)
	cleanup := func() { stop(); cancel(nil) }
	if err != nil {
		// HTTP/2 can return context.Canceled even when our timer cancelled the
		// attempt with DeadlineExceeded. Preserve that cause before cleanup so
		// callers do not mistake a proxy timeout for a disconnected client.
		if cause := context.Cause(attemptCtx); cause != nil {
			err = errors.Join(err, cause)
		}
		cleanup()
		return nil, started, stop, err
	}
	resp.Body = &cancelBody{ReadCloser: resp.Body, cancel: cleanup}
	return resp, started, stop, nil
}

type cancelBody struct {
	io.ReadCloser
	cancel func()
}

func (b *cancelBody) Close() error {
	b.cancel()
	return b.ReadCloser.Close()
}

// Error-body archiving must obey the same byte-idle bound as SSE capture.
// Closing the response body interrupts a blocked transport Read.
type byteIdleReader struct {
	body     io.ReadCloser
	timer    *time.Timer
	idle     time.Duration
	timedOut atomic.Bool
}

func withByteIdle(body io.ReadCloser, idle time.Duration) *byteIdleReader {
	r := &byteIdleReader{body: body, idle: idle}
	r.timer = time.AfterFunc(idle, func() {
		r.timedOut.Store(true)
		body.Close()
	})
	return r
}

func (r *byteIdleReader) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	if r.timedOut.Load() {
		return n, context.DeadlineExceeded
	}
	if n > 0 {
		r.timer.Reset(r.idle)
	}
	return n, err
}

func (r *byteIdleReader) stop() { r.timer.Stop() }
