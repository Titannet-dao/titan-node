package node

import (
	"math"

	"github.com/docker/go-units"
)

// 融合带宽值
func mergeBandwidth(nodeBW, serverBW, alpha float64) int64 {
	if alpha < 0 {
		alpha = 0
	} else if alpha > 1 {
		alpha = 1
	}

	mergedBW := nodeBW*alpha + serverBW*(1-alpha)
	return int64(mergedBW)
}

// 计算速度表现分数
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

// func calcCurScore(curBandwidth int64, succeedCount, totalCount int64, historyBandwidths []float64) int64 {
// 	mode := findMode(historyBandwidths)
// 	bandwidthSpeedScore := calcSpeedScore(curBandwidth)
// 	speedScore := (float64(curBandwidth) / float64(mode)) * 100 * (float64(bandwidthSpeedScore) / 100)

// 	serviceScore := 0.0
// 	if totalCount > 0 {
// 		serviceScore = (float64(succeedCount) / float64(totalCount) * 100) * 0.2
// 	}

// 	mean := calculateMean(historyBandwidths)
// 	stdDev := calculateStdDev(historyBandwidths, mean)
// 	jitterRate := (stdDev / mean) * 100
// 	stabilityScore := (100 - jitterRate) * 0.2

// 	totalScore := int64(speedScore + serviceScore + stabilityScore)
// 	return totalScore
// }

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

// 求众数
func findMode(values []int64) int64 {
	// 定义分组因子
	const factor = 5
	// 创建一个哈希表来存储每个分组的数量
	groupCountMap := make(map[int64][]int64)
	// 遍历数组,将每个元素分组
	for _, num := range values {
		group := num / factor
		groupCountMap[group] = append(groupCountMap[group], num)
	}
	// 找出数量最多的分组
	maxGroupIndex := int64(0)
	maxCount := 0

	for group, nums := range groupCountMap {
		count := len(nums)
		if count > maxCount {
			maxCount = count
			maxGroupIndex = group
		}
	}
	// // 如果有多个分组数量相同,则返回其中任意一个分组内的任意一个元素
	// if maxCount == 1 {
	// 	for _, num := range groupCountMap[maxGroup] {
	// 		return num
	// 	}
	// }
	// 否则,返回数量最多分组内所有元素的平均值
	var sum int64
	for _, num := range groupCountMap[maxGroupIndex] {
		sum += num
	}
	return sum / int64(maxCount)
}

// calculateMean 计算平均值
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

// calculateStdDev 计算标准差
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
