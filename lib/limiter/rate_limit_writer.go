package limiter

import (
	"fmt"
	"net/http"
	"time"

	"golang.org/x/time/rate"
)

type writer struct {
	w        http.ResponseWriter
	limiter  *rate.Limiter
	rate     int64
	interval time.Duration
}

// NewReader returns a reader that is rate limited by
// the given token bucket. Each token in the bucket
// represents one byte.
func NewWriter(resWriter http.ResponseWriter, l *rate.Limiter) *writer {
	return &writer{
		w:       resWriter,
		limiter: l,
	}
}

func (w *writer) Write(buf []byte) (int, error) {
	now := time.Now()
	rv := w.limiter.ReserveN(now, len(buf))
	if !rv.OK() {
		fmt.Println("exceeds limiter's burst")
		return 0, fmt.Errorf("exceeds limiter's burst")
	}

	delay := rv.DelayFrom(now)
	time.Sleep(delay)

	return w.w.Write(buf)
}

func (w *writer) Header() http.Header {
	return w.w.Header()
}

func (w *writer) WriteHeader(statusCode int) {
	w.w.WriteHeader(statusCode)
}
