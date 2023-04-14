package etcdcli

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/modules/dtypes"

	"go.etcd.io/etcd/clientv3"
	"golang.org/x/xerrors"
)

const (
	connectServerTimeoutTime = 5  // Second
	serverKeepAliveDuration  = 10 // Second
)

// Client ...
type Client struct {
	cli *clientv3.Client
}

// New new a etcd client
func New(addrs []string) (*Client, error) {
	config := clientv3.Config{
		Endpoints:   addrs,
		DialTimeout: connectServerTimeoutTime * time.Second,
	}
	// connect
	cli, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	client := &Client{
		cli: cli,
	}

	return client, nil
}

// ServerRegister register to etcd , If already register in, return an error
func (c *Client) ServerRegister(t context.Context, serverID dtypes.ServerID, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	serverKey := fmt.Sprintf("/%s/%s", types.RunningNodeType.String(), serverID)

	// get a lease
	lease := clientv3.NewLease(c.cli)
	leaseRsp, err := lease.Grant(ctx, serverKeepAliveDuration)
	if err != nil {
		return xerrors.Errorf("Grant lease err:%s", err.Error())
	}

	leaseID := leaseRsp.ID

	kv := clientv3.NewKV(c.cli)
	// Create transaction
	txn := kv.Txn(ctx)

	// If the revision of key is equal to 0
	txn.If(clientv3.Compare(clientv3.CreateRevision(serverKey), "=", 0)).
		Then(clientv3.OpPut(serverKey, value, clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(serverKey))

	// Commit transaction
	txnResp, err := txn.Commit()
	if err != nil {
		return err
	}

	// already exists
	if !txnResp.Succeeded {
		return xerrors.Errorf("Server key already exists")
	}

	// KeepAlive
	keepRespChan, err := lease.KeepAlive(context.TODO(), leaseID)
	if err != nil {
		return err
	}
	// lease keepalive response queue capacity only 16 , so need to read it
	go func() {
		for {
			<-keepRespChan
		}
	}()

	return nil
}

// WatchServers watch server login and logout
func (c *Client) WatchServers(ctx context.Context, nodeType types.NodeType) clientv3.WatchChan {
	prefix := fmt.Sprintf("/%s/", nodeType.String())

	watcher := clientv3.NewWatcher(c.cli)
	watchRespChan := watcher.Watch(ctx, prefix, clientv3.WithPrefix())

	return watchRespChan
}

// ListServers list server
func (c *Client) GetServers(nodeType types.NodeType) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	serverKeyPrefix := fmt.Sprintf("/%s/", nodeType.String())
	kv := clientv3.NewKV(c.cli)

	return kv.Get(ctx, serverKeyPrefix, clientv3.WithPrefix())
}

// ServerUnRegister UnRegister to etcd
func (c *Client) ServerUnRegister(t context.Context, serverID dtypes.ServerID) error {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	kv := clientv3.NewKV(c.cli)

	serverKey := fmt.Sprintf("/%s/%s", types.RunningNodeType.String(), serverID)

	_, err := kv.Delete(ctx, serverKey)
	return err
}

// SCUnmarshal  Unmarshal SchedulerCfg
func SCUnmarshal(v []byte) (*types.SchedulerCfg, error) {
	s := &types.SchedulerCfg{}
	err := json.Unmarshal(v, s)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// SCMarshal  Marshal SchedulerCfg
func SCMarshal(s *types.SchedulerCfg) ([]byte, error) {
	v, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return v, nil
}
