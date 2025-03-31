package node

import (
	"math"

	"github.com/docker/go-units"
)

func calcSpeedScore(bandwidth int64) int64 {
	var maxScore int64
	switch {
	case bandwidth > 500*units.MiB:
		maxScore = 90
	case bandwidth > 200*units.MiB:
		maxScore = 70
	case bandwidth > 100*units.MiB:
		maxScore = 50
	case bandwidth > 50*units.MiB:
		maxScore = 30
	case bandwidth > 10*units.MiB:
		maxScore = 20
	default:
		maxScore = 10
	}

	return maxScore
}

func calcScore(curScore float64, historyScoresHour, historyScoresDay, historyScoresWeek []float64) int64 {
	scoreCur := float64(curScore) * 0.4

	historyScoresHour = append(historyScoresHour, curScore)
	scoreHour := calculateMean(historyScoresHour) * 0.3

	historyScoresDay = append(historyScoresDay, curScore)
	scoreDay := calculateMean(historyScoresDay) * 0.2

	historyScoresWeek = append(historyScoresWeek, curScore)
	scoreWeek := calculateMean(historyScoresWeek) * 0.1

	return int64(scoreCur + scoreHour + scoreDay + scoreWeek)
}

// calculateMean
func calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// calculateStdDev
func calculateStdDev(values []float64, mean float64) float64 {
	if len(values) < 2 {
		return 0
	}

	var sumSquares float64
	for _, v := range values {
		diff := v - mean
		sumSquares += diff * diff
	}
	return math.Sqrt(sumSquares / float64(len(values)-1))
}
