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
	resp.Body = &cancelBody{ReadCloser: resp.Body, ctx: attemptCtx, cancel: cleanup}
	return resp, started, stop, nil
}

type cancelBody struct {
	io.ReadCloser
	ctx    context.Context
	cancel func()
}

func (b *cancelBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		if cause := context.Cause(b.ctx); cause != nil {
			err = errors.Join(err, cause)
		}
	}
	return n, err
}

func (b *cancelBody) Close() error {
	b.cancel()
	return b.ReadCloser.Close()
}

// Bound time spent waiting for upstream bytes, not time spent writing them to
// a slow downstream or archive. Closing the body interrupts a blocked Read.
type byteIdleReader struct {
	body     io.ReadCloser
	timer    *time.Timer
	idle     time.Duration
	timedOut atomic.Bool
}

func withByteIdle(body io.ReadCloser, idle time.Duration) *byteIdleReader {
	return &byteIdleReader{body: body, idle: idle}
}

func (r *byteIdleReader) Read(p []byte) (int, error) {
	if r.timedOut.Load() {
		return 0, context.DeadlineExceeded
	}
	if r.timer == nil {
		r.timer = time.AfterFunc(r.idle, func() {
			r.timedOut.Store(true)
			r.body.Close()
		})
	} else {
		r.timer.Reset(r.idle)
	}
	n, err := r.body.Read(p)
	r.timer.Stop()
	if r.timedOut.Load() {
		return n, context.DeadlineExceeded
	}
	return n, err
}

func (r *byteIdleReader) stop() {
	if r.timer != nil {
		r.timer.Stop()
	}
}

// flushingWriter preserves incremental delivery for single-shot SSE responses.
type flushingWriter struct {
	w       io.Writer
	control *http.ResponseController
}

func (w flushingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if err == nil {
		err = w.control.Flush()
	}
	return n, err
}
