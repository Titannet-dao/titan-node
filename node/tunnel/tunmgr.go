package tunnel

import (
	"sync"
	"time"
)

const (
	keepaliveIntervel = 10 * time.Second
	keepaliveTimeout  = 3 * keepaliveIntervel
)

type TunManager struct {
	tunnels *sync.Map
}

func (tm *TunManager) addTunnel(tunnel *Tunnel) {
	tm.tunnels.Store(tunnel.ID, tunnel)
}

func (tm *TunManager) removeTunnel(tunnel *Tunnel) {
	tm.tunnels.Delete(tunnel.ID)
}

func (tm *TunManager) getTunnel(id string) *Tunnel {
	v, ok := tm.tunnels.Load(id)
	if !ok {
		return nil
	}
	return v.(*Tunnel)
}

func (tm *TunManager) keepAlive() {
	ticker := time.NewTicker(keepaliveIntervel)

	for {
		<-ticker.C
		tm.tunnels.Range(func(key, value interface{}) bool {
			tunnel, ok := value.(*Tunnel)
			if !ok {
				log.Errorf("convert value to Tunnel failed")
				return true
			}

			if time.Since(tunnel.lastActivitTime) > keepaliveTimeout {
				tunnel.conn.Close()
				log.Infof("tunnel client %s offline", tunnel.ID)
			}
			return true
		})

	}
}
