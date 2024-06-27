package node

import (
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
)

const (
	uploadTrafficProfit   = 220.0 // p/GB
	downloadTrafficProfit = 55.0  // p/GB

	mr = 1.0
	mo = 1.0

	phoneWeighting = 5.0

	l1RBase     = 200000.0 / 288.0 // every 5 minutes
	penaltyRate = 0.0001

	exitRate = 0.6

	l2PhoneRate = 500.0 / 17280.0 // every 5s
	l2PCRate    = 200.0 / 17280.0 // every 5s
	l2OtherRate = 50.0 / 17280.0  // every 5s
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

// func (m *Manager) startMxTimer() {
// 	ticker := time.NewTicker(time.Minute * 1)
// 	defer ticker.Stop()

// 	mx = updateMx()

// 	for {
// 		<-ticker.C
// 		mx = updateMx()
// 	}
// }

// func (m *Manager) GetNodeBaseProfitDetails(node *Node, count float64) *types.ProfitDetails {
// 	p := count * m.NodeCalculateMCx()

// 	if p < 0.000001 {
// 		return nil
// 	}

// 	return &types.ProfitDetails{
// 		NodeID: node.NodeID,
// 		Profit: p,
// 		PType:  types.ProfitTypeBase,
// 	}
// }

// func (m *Manager) GetNodePullProfitDetails(node *Node, size float64, note string) *types.ProfitDetails {
// 	w := 1.0
// 	if node.IsPhone {
// 		w = phoneWeighting
// 	}

// 	d := bToGB(size)
// 	mip := calculateMip(node.NATType)
// 	lip := len(m.GetNodeOfIP(node.ExternalIP))
// 	mn := calculateMn(lip)

// 	mbnd := mr * mx * mo * d * downloadTrafficProfit * mip * mn * w

// 	if mbnd < 0.000001 {
// 		return nil
// 	}

// 	return &types.ProfitDetails{
// 		NodeID: node.NodeID,
// 		Profit: mbnd,
// 		PType:  types.ProfitTypePull,
// 		Size:   int64(size),
// 		Note:   fmt.Sprintf("lip:[%d] ; mr:[%.4f], mx:[%.4f], mo:[%.4f], d:[%.6f]GB, [%.4f], mip:[%.4f], mn:[%.4f] ,w:[%.1f] ", lip, mr, mx, mo, d, downloadTrafficProfit, mip, mn, w),
// 	}
// }

func (m *Manager) GetNodeBePullProfitDetails(node *Node, size float64, note string) *types.ProfitDetails {
	u := bToGB(size)
	b := calculateB(node.BandwidthUp)
	mip := calculateMip(node.NATType)
	lip := len(m.GetNodeOfIP(node.ExternalIP))
	mn := calculateMn(lip)
	mx := rateOfL2Mx(node.OnlineDuration)

	mbnu := mr * mx * mo * u * b * uploadTrafficProfit * mip * mn

	if mbnu < 0.000001 {
		return nil
	}

	return &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: mbnu,
		PType:  types.ProfitTypeBePull,
		Size:   int64(size),
		Note:   fmt.Sprintf("lip:[%d] BandwidthUp:[%d]; mr:[%.4f], mx:[%.4f], mo:[%.4f], u:[%.6f]GB, b:[%.4f], [%.4f], mip:[%.4f], mn:[%.4f]", lip, node.BandwidthUp, mr, mx, mo, u, b, uploadTrafficProfit, mip, mn),
	}
}

// func (m *Manager) GetNodeValidatorProfitDetails(node *Node, size float64) *types.ProfitDetails {
// 	w := 1.0
// 	if node.IsPhone {
// 		w = phoneWeighting
// 	}

// 	d := bToGB(size)

// 	mip := calculateMip(node.NATType)
// 	lip := len(m.GetNodeOfIP(node.ExternalIP))
// 	mn := calculateMn(lip)

// 	mbnd := mr * mx * mo * d * downloadTrafficProfit * mip * mn * w

// 	if mbnd < 0.000001 {
// 		return nil
// 	}

// 	return &types.ProfitDetails{
// 		NodeID: node.NodeID,
// 		Profit: mbnd,
// 		PType:  types.ProfitTypeValidator,
// 		Size:   int64(size),
// 		Note:   fmt.Sprintf("lip:[%d] ; mr:[%.4f], mx:[%.4f], mo:[%.4f], d:[%.4f], [%.4f], mip:[%.4f], mn:[%.4f],w:[%.1f]", lip, mr, mx, mo, d, downloadTrafficProfit, mip, mn, w),
// 	}
// }

func (m *Manager) GetNodeValidatableProfitDetails(node *Node, size float64) *types.ProfitDetails {
	ds := float64(node.TitanDiskUsage)
	s := bToGB(ds)
	u := bToGB(size)
	mx := rateOfL2Mx(node.OnlineDuration)
	mt := 1.0
	b := calculateB(node.BandwidthUp)
	mip := calculateMip(node.NATType)
	lip := len(m.GetNodeOfIP(node.ExternalIP))
	mn := calculateMn(lip)

	ms := (mr * mx * mo * min(s, 50) * 0.211148649 * mt) + (u * b * uploadTrafficProfit * mip * mn)

	if ms < 0.000001 {
		return nil
	}

	return &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: ms,
		PType:  types.ProfitTypeValidatable,
		Size:   int64(size),
		Note:   fmt.Sprintf("lip:[%d] BandwidthUp:[%d]; mr:[%.4f], mx:[%.4f], mo:[%.4f], s:[%.4f], u:[%.6f]GB, b:[%.4f], [%.4f], mip:[%.4f], mn:[%.4f]", lip, node.BandwidthUp, mr, mx, mo, s, u, b, uploadTrafficProfit, mip, mn),
	}
}

func (m *Manager) GetEdgeBaseProfitDetails(node *Node) (float64, *types.ProfitDetails) {
	// Every 5 s
	mx := rateOfL2Mx(node.OnlineDuration)
	mb := rateOfL2Base(node.ClientType)
	mcx := mr * mx * mo * mb

	p := mcx * float64(node.KeepaliveCount)

	return mcx, &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: p,
		PType:  types.ProfitTypeBase,
		Note:   fmt.Sprintf(" mr:[%.1f], mx:[%0.4f], mo:[%0.1f], mb:[%.4f],count:[%d] ClientType:[%d] , online min:[%d]", mr, mx, mo, mb, node.KeepaliveCount, node.ClientType, node.OnlineDuration),
	}
}

func (m *Manager) GetCandidateBaseProfitDetails(node *Node) *types.ProfitDetails {
	// Every 5 minutes
	arR := rateOfAR(node.OnlineRate)
	mcx := l1RBase * node.OnlineRate * arR

	return &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: mcx,
		PType:  types.ProfitTypeBase,
		Note:   fmt.Sprintf("rbase:[%.4f],node rate:[%.4f] ar rate:[%.4f]", l1RBase, node.OnlineRate, arR),
	}
}

func (m *Manager) GetDownloadProfitDetails(node *Node, size float64) *types.ProfitDetails {
	d := bToGB(size)
	mx := rateOfL2Mx(node.OnlineDuration)
	lip := len(m.GetNodeOfIP(node.ExternalIP))
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
		Note:   fmt.Sprintf("lip:[%d] ; mr:[%.4f], mx:[%.4f], mo:[%.4f], d:[%.4f]GB, [%.4f], mn:[%.4f]", lip, mr, mx, mo, d, downloadTrafficProfit, mn),
	}
}

func (m *Manager) GetUploadProfitDetails(node *Node, size float64) *types.ProfitDetails {
	u := bToGB(size)
	mx := rateOfL2Mx(node.OnlineDuration)
	b := calculateB(node.BandwidthUp)
	mip := calculateMip(node.NATType)
	lip := len(m.GetNodeOfIP(node.ExternalIP))
	mn := calculateMn(lip)

	ms := (mr * mx * mo * u * b * uploadTrafficProfit * mn)

	if ms < 0.000001 {
		return nil
	}

	return &types.ProfitDetails{
		NodeID: node.NodeID,
		Profit: ms,
		PType:  types.ProfitTypeUpload,
		Size:   int64(size),
		Note:   fmt.Sprintf("lip:[%d] BandwidthUp:[%d]; mr:[%.4f], mx:[%.4f], mo:[%.4f], u:[%.6f]GB, b:[%.4f], [%.4f], mip:[%.4f], mn:[%.4f]", lip, node.BandwidthUp, mr, mx, mo, u, b, uploadTrafficProfit, mip, mn),
	}
}

func (m *Manager) CalculatePenalty(nodeID string, profit float64, offlineDuration int) float64 {
	od := float64(offlineDuration / 200)
	pr := (penaltyRate + penaltyRate*od)
	pn := profit * pr

	log.Infof("CalculatePenalty %s ,pn:[%.4f]  profit:[%.4f] pr:[%.4f] od:[%.4f] , offlineDuration:[%d]", nodeID, pn, profit, pr, od, offlineDuration)

	return pn
}

func (m *Manager) CalculateExitProfit(profit float64) (float64, float64) {
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

func calculateB(upload int64) float64 {
	mb := bToMB(float64(upload))
	if mb >= 30 {
		return 1.2
	}

	if mb >= 5 {
		return 1
	}

	return 0.8
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

func rateOfL2Base(nct types.NodeClientType) float64 {
	switch nct {
	case types.NodeAndroid, types.NodeIOS:
		return l2PhoneRate
	case types.NodeMacos, types.NodeWindows:
		return l2PCRate
	}

	return l2OtherRate
}

func rateOfL2Mx(onlineDuration int) float64 {
	onlineHour := onlineDuration / 60

	if onlineHour < 24 {
		return 0
	} else if onlineHour < 48 {
		return 0.15
	} else if onlineHour < 72 {
		return 0.3
	} else if onlineHour < 96 {
		return 0.5
	} else if onlineHour < 120 {
		return 0.7
	} else if onlineHour < 144 {
		return 0.9
	} else if onlineHour < 168 {
		return 1
	}

	onlineDay := onlineHour / 24

	count := onlineDay - 7

	return 1.0 + (float64(count) * 0.03)
}
