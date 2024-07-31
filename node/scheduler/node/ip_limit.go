package node

import "sync"

// IPMgr node ip info manager
type IPMgr struct {
	ipLimit int
	nodeIPs sync.Map
}

func newIPMgr(limit int) *IPMgr {
	return &IPMgr{
		ipLimit: limit,
	}
}

// StoreNodeIP store node
func (m *IPMgr) StoreNodeIP(nodeID, ip string) bool {
	listI, exist := m.nodeIPs.Load(ip)
	if exist {
		list := listI.([]string)

		for _, nID := range list {
			if nID == nodeID {
				return true
			}
		}

		if len(list) < m.ipLimit {
			list = append(list, nodeID)
			m.nodeIPs.Store(ip, list)
			return true
		}

		return false
	}

	m.nodeIPs.Store(ip, []string{nodeID})
	return true
}

// RemoveNodeIP remove node
func (m *IPMgr) RemoveNodeIP(nodeID, ip string) {
	listI, exist := m.nodeIPs.Load(ip)
	if exist {
		list := listI.([]string)
		nList := []string{}

		for _, nID := range list {
			if nID != nodeID {
				nList = append(nList, nID)
			}
		}

		m.nodeIPs.Store(ip, nList)
	}
}

// GetNodeOfIP get node
func (m *IPMgr) GetNodeOfIP(ip string) []string {
	listI, exist := m.nodeIPs.Load(ip)
	if exist {
		list := listI.([]string)
		return list
	}

	return []string{}
}

// CheckIPExist check node
func (m *IPMgr) CheckIPExist(ip string) bool {
	_, exist := m.nodeIPs.Load(ip)

	return exist
}
