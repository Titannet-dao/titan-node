package httpserver

import (
	"net/http"
	"time"
)

type SpeedCountWriter struct {
	w         http.ResponseWriter
	dataSize  int64
	startTime time.Time
	lastTime  time.Time
	peakSpeed int64
}

func (w *SpeedCountWriter) Header() http.Header {
	return w.w.Header()
}

func (w *SpeedCountWriter) Write(bytes []byte) (int, error) {
	now := time.Now()

	if w.startTime.IsZero() {
		w.startTime = now
	}

	if w.lastTime.IsZero() {
		w.lastTime = now
	}

	w.dataSize += int64(len(bytes))

	// calculate peak speed
	// ignore on first sample
	if w.lastTime != w.startTime {

		duration := now.Sub(w.lastTime)
		w.lastTime = now
		if duration > 0 {
			curSpeed := int64(float64(len(bytes)) / duration.Seconds())
			if curSpeed > w.peakSpeed {
				w.peakSpeed = curSpeed
			}
		}

	}

	return w.w.Write(bytes)
}

func (w *SpeedCountWriter) WriteHeader(statusCode int) {
	w.w.WriteHeader(statusCode)
}

// ms
func (w *SpeedCountWriter) CostTime() float64 {
	return float64(time.Since(w.startTime)) / float64(time.Millisecond)
}

func (w *SpeedCountWriter) Speed() int64 {
	if w.dataSize == 0 {
		return 0
	}

	duration := time.Since(w.startTime)
	if duration > 0 {
		return int64(float64(w.dataSize) / duration.Seconds())
	}

	return 0
}

func (w *SpeedCountWriter) PeakSpeed() int64 {
	return w.peakSpeed
}
