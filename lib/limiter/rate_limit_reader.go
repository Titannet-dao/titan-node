package limiter

import (
	"fmt"
	"io"
	"time"

	"golang.org/x/time/rate"
)

type reader struct {
	rs      io.ReadSeeker
	limiter *rate.Limiter
}

// NewReader returns a reader that is rate limited by
// the given token bucket. Each token in the bucket
// represents one byte.
func NewReader(rs io.ReadSeeker, l *rate.Limiter) io.ReadSeeker {
	return &reader{
		rs:      rs,
		limiter: l,
	}
}

func (r *reader) Read(buf []byte) (int, error) {
	n, err := r.rs.Read(buf)
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

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	return r.rs.Seek(offset, whence)
}
