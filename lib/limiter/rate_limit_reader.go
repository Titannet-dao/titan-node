package limiter

import (
	"fmt"
	"io"
	"time"

	"golang.org/x/time/rate"
)

type reader struct {
	r       io.Reader
	limiter *rate.Limiter
}

// NewReader returns a reader that is rate limited by
// the given token bucket. Each token in the bucket
// represents one byte.
func NewReader(r io.Reader, l *rate.Limiter) io.Reader {
	return &reader{
		r:       r,
		limiter: l,
	}
}

func (r *reader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)
	if n <= 0 {
		return n, err
	}

	now := time.Now()
	rv := r.limiter.ReserveN(now, n)
	if !rv.OK() {
		return 0, fmt.Errorf("exceeds limiter's burst")
	}

	delay := rv.DelayFrom(now)
	time.Sleep(delay)
	return n, err
}
