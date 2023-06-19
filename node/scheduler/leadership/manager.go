package leadership

import (
	"bytes"
	"encoding/binary"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"github.com/Filecoin-Titan/titan/node/repo"
	nTypes "github.com/Filecoin-Titan/titan/node/types"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("leadership")

const (
	masterLeaseID = "master-lease-id"
)

// Manager is the node manager responsible for managing the online nodes
type Manager struct {
	lr      repo.LockedRepo
	etcdcli *etcdcli.Client

	isMaster bool
}

// NewManager creates a new instance of the node manager
func NewManager(lr repo.LockedRepo, ec *etcdcli.Client) *Manager {
	manager := &Manager{
		lr:      lr,
		etcdcli: ec,
	}

	return manager
}

// RequestAndBecomeMaster tries to acquire a lock from etcd. If the lock is successfully acquired,
// the function sets the server as the master and returns true. Otherwise, it returns false.
func (m *Manager) RequestAndBecomeMaster() bool {
	if m.isMaster {
		return true
	}

	oldLeaseID, err := m.getStore()
	if err != nil {
		log.Errorf("RequestAndBecomeMaster getStore err:%s", err.Error())
	}

	// Try to acquire the master lock in etcd using the old lease ID.
	newLeaseID, err := m.etcdcli.AcquireMasterLock(types.RunningNodeType.String(), oldLeaseID)
	if err != nil {
		log.Errorf("RequestAndBecomeMaster SetMasterScheduler err:%s", err.Error())
		return m.isMaster
	}

	m.isMaster = true

	err = m.putStore(newLeaseID)
	if err != nil {
		log.Errorf("RequestAndBecomeMaster putStore err:%s", err.Error())
	}

	return m.isMaster
}

func (m *Manager) getStore() (int64, error) {
	keystore, err := m.lr.KeyStore()
	if err != nil {
		return 0, err
	}

	kInfo, err := keystore.Get(masterLeaseID)
	if err != nil {
		return 0, err
	}

	return byteToInt(kInfo.PrivateKey)
}

func (m *Manager) putStore(leaseID int64) error {
	keystore, err := m.lr.KeyStore()
	if err != nil {
		return err
	}

	k, err := intToByte(leaseID)
	if err != nil {
		return err
	}

	kInfo := nTypes.KeyInfo{
		Type:       masterLeaseID,
		PrivateKey: k,
	}

	keystore.Delete(masterLeaseID)

	return keystore.Put(masterLeaseID, kInfo)
}

func intToByte(n int64) ([]byte, error) {
	byteBuf := bytes.NewBuffer([]byte{})
	err := binary.Write(byteBuf, binary.BigEndian, n)

	return byteBuf.Bytes(), err
}

func byteToInt(bys []byte) (int64, error) {
	byteBuf := bytes.NewBuffer(bys)
	var i int64
	err := binary.Read(byteBuf, binary.BigEndian, &i)

	return i, err
}
