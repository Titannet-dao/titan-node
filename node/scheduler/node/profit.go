package node

import (
	"fmt"
	"math"

	"github.com/Filecoin-Titan/titan/api/types"
)

const (
	validatableUploadTrafficProfit = 15.0 // p/GB
	uploadTrafficProfit            = 50.0 // p/GB
	downloadTrafficProfit          = 30.0 // p/GB

	mr = 1.0
	mo = 1.0

	phoneWeighting = 5.0

	l1RBase = 200000.0 / 1440.0 // every 1 minutes
	// l1RBase     = 200000.0 / 288.0 // every 5 minutes
	penaltyRate = 0.0001

	exitRate = 0.6

	l2PhoneRate = 500.0 / 17280.0 // every 5s
	l2PCRate    = 300.0 / 17280.0 // every 5s
	l2OtherRate = 150.0 / 17280.0 // every 5s

	l2ProfitLimit = 7000
)

var (
// mx = 1.0

// year = time.Now().UTC().Year()

// week1Start = time.Date(year, time.April, 22, 8, 0, 0, 0, time.UTC)
// week1End   = time.Date(year, time.April, 28, 23, 59, 59, 0, time.UTC)
// week2Start = time.Date(year, time.April, 29, 0, 0, 0, 0, time.UTC)
// week2End   = time.Date(year, time.May, 5, 23, 59, 59, 0, time.UTC)
// week3Start = time.Date(year, time.May, 6, 0, 0, 0, 0, time.UTC)
// week3End   = time.Date(year, time.May, 12, 23, 59, 59, 0, time.UTC)
// week4Start = time.Date(year, time.May, 13, 0, 0, 0, 0, time.UTC)
// week4End   = time.Date(year, time.May, 19, 23, 59, 59, 0, time.UTC)
)

// func updateMx() float64 {
// 	now := time.Now().UTC()

// 	log.Debugf("updateMx now : %s , %.2f", now.Format(time.DateTime), mx)

// 	switch {
// 	case now.After(week1Start) && now.Before(week1End):
// 		return 2
// 	case now.After(week2Start) && now.Before(week2End):
// 		return 1.7
// 	case now.After(week3Start) && now.Before(week3End):
// 		return 1.4
// 	case now.After(week4Start) && now.Before(week4End):
// 		return 1.2
// 	default:
// 		return 1
// 	}
// }

func (m *Manager) isExceededLimit(nodeID string) bool {
	todayProfit, err := m.LoadTodayProfitsForNode(nodeID)
	if err != nil {
		log.Errorf("%s LoadTodayProfitsForNode err:%s", nodeID, err.Error())
		return true
	}

	if todayProfit > l2ProfitLimit {
		log.Infof("%s LoadTodayProfitsForNode %.4f > %d", nodeID, todayProfit, l2ProfitLimit)
		return true
	}

	return false
}

// GetNodeBePullProfitDetails Provide download rewards
func (m *Manager) GetNodeBePullProfitDetails(node *Node, size float64, note string) *types.ProfitDetails {
	if m.isExceededLimit(node.NodeID) {
		return nil
	}

	u := bToGB(size)
	mip := calculateMip(node.NATType)
	lip := len(m.IPMgr.GetNodeOfIP(node.ExternalIP))
	mn := calculateMn(lip)
	mx := RateOfL2Mx(node.OnlineDuration)

	mbnu := mr * mx * mo * u * uploadTrafficProfit * mip * mn

	if mbnu < 0.000001 {
		return nil
	}

	return &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: mbnu,
		PType:  types.ProfitTypeBePull,
		Size:   int64(size),
		Note:   fmt.Sprintf("lip:[%d]  mr:[%.4f], mx:[%.4f], mo:[%.4f], u:[%.6f]GB, [%.2f], mip:[%.4f], mn:[%.4f]", lip, mr, mx, mo, u, uploadTrafficProfit, mip, mn),
	}
}

// GetNodeValidatableProfitDetails Rewards for random inspection
func (m *Manager) GetNodeValidatableProfitDetails(node *Node, size float64) *types.ProfitDetails {
	ds := float64(node.TitanDiskUsage)
	s := bToGB(ds)
	u := bToGB(size)
	mx := RateOfL2Mx(node.OnlineDuration)
	mt := 1.0
	mip := calculateMip(node.NATType)
	lip := len(m.IPMgr.GetNodeOfIP(node.ExternalIP))
	mn := calculateMn(lip)

	ms := (mr * mx * mo * min(s, 50) * 1.5 * mt) + (u * validatableUploadTrafficProfit * mip * mn)

	if ms < 0.000001 {
		return nil
	}

	return &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: ms,
		PType:  types.ProfitTypeValidatable,
		Size:   int64(size),
		Note:   fmt.Sprintf("lip:[%d] mr:[%.4f], mx:[%.4f], mo:[%.4f], s:[%.4f], u:[%.6f]GB, [%.1f], mip:[%.4f], mn:[%.4f]", lip, mr, mx, mo, s, u, validatableUploadTrafficProfit, mip, mn),
	}
}

// GetEdgeBaseProfitDetails Basic Rewards
func (m *Manager) GetEdgeBaseProfitDetails(node *Node, minute int) (float64, *types.ProfitDetails) {
	// Every 5 s
	mx := RateOfL2Mx(node.OnlineDuration)
	mb := rateOfL2Base(node.ClientType)
	mcx := mr * mx * mo * mb

	count := minute * 12
	p := mcx * float64(count)

	// client incr
	cIncr := mcx * 360

	if p < 0.000001 {
		return cIncr, nil
	}

	return cIncr, &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: p,
		PType:  types.ProfitTypeBase,
		Note:   fmt.Sprintf(" mr:[%.1f], mx:[%0.4f], mo:[%0.1f], mb:[%.4f],count:[%d] ClientType:[%d] , online min:[%d]", mr, mx, mo, mb, count, node.ClientType, node.OnlineDuration),
	}
}

// GetCandidateBaseProfitDetails Basic Rewards
func (m *Manager) GetCandidateBaseProfitDetails(node *Node, minute int) *types.ProfitDetails {
	// Every 1 minutes
	arR := rateOfAR(node.OnlineRate)
	arO := rateOfOnline(node.OnlineDuration)
	mcx := l1RBase * node.OnlineRate * arR * arO

	// count := roundDivision(minute, 1)
	mcx = mcx * float64(minute)

	if mcx < 0.000001 {
		return nil
	}

	return &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: mcx,
		PType:  types.ProfitTypeBase,
		Note:   fmt.Sprintf("rbase:[%.4f],node rate:[%.4f] ar rate:[%.4f] arO:[%.2f], count[%d], online[%d]", l1RBase, node.OnlineRate, arR, arO, minute, node.OnlineDuration),
	}
}

// GetDownloadProfitDetails Downstream traffic reward
func (m *Manager) GetDownloadProfitDetails(node *Node, size float64, pid string) *types.ProfitDetails {
	if m.isExceededLimit(node.NodeID) {
		return nil
	}

	d := bToGB(size)
	mx := RateOfL2Mx(node.OnlineDuration)
	lip := len(m.IPMgr.GetNodeOfIP(node.ExternalIP))
	mn := calculateMn(lip)

	ms := mr * mx * mo * d * downloadTrafficProfit * mn

	if ms < 0.000001 {
		return nil
	}

	return &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: ms,
		PType:  types.ProfitTypeDownload,
		Size:   int64(size),
		CID:    pid,
		Note:   fmt.Sprintf("lip:[%d] ; mr:[%.4f], mx:[%.4f], mo:[%.4f], d:[%.4f]GB, [%.4f], mn:[%.4f], Profit:[%.4f]", lip, mr, mx, mo, d, downloadTrafficProfit, mn, ms),
	}
}

// GetUploadProfitDetails Upstream traffic reward
func (m *Manager) GetUploadProfitDetails(node *Node, size float64, pid string) *types.ProfitDetails {
	if m.isExceededLimit(node.NodeID) {
		return nil
	}

	u := bToGB(size)
	mx := RateOfL2Mx(node.OnlineDuration)
	mip := calculateMip(node.NATType)
	lip := len(m.IPMgr.GetNodeOfIP(node.ExternalIP))
	mn := calculateMn(lip)

	ms := mr * mx * mo * u * uploadTrafficProfit * mn

	if ms < 0.000001 {
		return nil
	}

	return &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: ms,
		PType:  types.ProfitTypeUpload,
		Size:   int64(size),
		CID:    pid,
		Note:   fmt.Sprintf("lip:[%d] mr:[%.4f], mx:[%.4f], mo:[%.4f], u:[%.6f]GB, [%.4f], mip:[%.4f], mn:[%.4f], Profit:[%.4f]", lip, mr, mx, mo, u, uploadTrafficProfit, mip, mn, ms),
	}
}

// CalculatePenalty Penalty
func (m *Manager) CalculatePenalty(nodeID string, profit float64, offlineDuration int, onlineDuration int) *types.ProfitDetails {
	onlineDay := (float64(onlineDuration) / 1440)
	if onlineDay > 30 {
		profit = profit / onlineDay * 30
	}

	od := float64(offlineDuration / 200)
	pr := (penaltyRate + penaltyRate*od)
	pn := profit * pr

	if pn > profit {
		pn = profit
	}

	return &types.ProfitDetails{
		NodeID:  nodeID,
		Profit:  -pn,
		Penalty: pn,
		PType:   types.ProfitTypeOfflinePenalty,
		Rate:    pr,
		Note:    fmt.Sprintf("pn:[%.4f]  profit:[%.4f] pr:[%.4f] od:[%.4f] , offlineDuration:[%d]", pn, profit, pr, od, offlineDuration),
	}
}

// GetRecompenseProfitDetails recompense
func (m *Manager) GetRecompenseProfitDetails(nodeID string, profit float64, note string) *types.ProfitDetails {
	if profit < 0.000001 {
		return nil
	}

	return &types.ProfitDetails{
		NodeID: nodeID,
		Profit: profit,
		PType:  types.ProfitTypeRecompense,
		Note:   note,
	}
}

// CalculateDowntimePenalty Exit penalty calculation
func (m *Manager) CalculateDowntimePenalty(profit float64) (float64, float64) {
	rExit := profit * (1 - exitRate)

	return rExit, exitRate
}

func calculateMip(n string) float64 {
	switch n {
	case types.NatTypeNo.String():
		return 2
	case types.NatTypeFullCone.String():
		return 1.5
	case types.NatTypeRestricted.String():
		return 1.3
	case types.NatTypePortRestricted.String():
		return 1.1
	case types.NatTypeSymmetric.String():
		return 0.8
	}

	return 0.8
}

func calculateMn(ipNum int) float64 {
	switch ipNum {
	case 1:
		return 1.1
	case 2:
		return 0.4
	case 3:
		return 0.3
	case 4:
		return 0.2
	}

	return 0.1
}

func rateOfAR(ar float64) float64 {
	if ar >= 0.98 {
		return 1.1
	} else if ar >= 0.95 {
		return 1
	} else if ar >= 0.85 {
		return 0.8
	} else if ar >= 0.3 {
		return 0.5
	}

	return 0
}

func rateOfOnline(onlineDuration int) float64 {
	onlineDay := onlineDuration / 1440
	if onlineDay >= 120 {
		return 1.3
	} else if onlineDay >= 90 {
		return 1.2
	} else if onlineDay >= 60 {
		return 1.1
	} else if onlineDay >= 30 {
		return 1.05
	}

	return 1
}

func rateOfL2Base(nct types.NodeClientType) float64 {
	switch nct {
	case types.NodeAndroid, types.NodeIOS:
		return l2PhoneRate
	case types.NodeMacos, types.NodeWindows:
		return l2PCRate
	}

	return l2OtherRate
}

// RateOfL2Mx mx
func RateOfL2Mx(onlineDuration int) float64 {
	onlineHour := onlineDuration / 60

	if onlineHour < 168 {
		return 1
	}

	onlineDay := onlineHour / 24
	count := onlineDay - 7

	rate := 1.0 + (float64(count) * 0.03)
	return math.Min(6, rate)
}

func bToGB(b float64) float64 {
	return b / 1024 / 1024 / 1024
}

func bToMB(b float64) float64 {
	return b / 1024 / 1024
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}

	return b
}
