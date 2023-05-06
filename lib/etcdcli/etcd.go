package etcdcli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"golang.org/x/xerrors"
)

const (
	connectServerTimeoutTime = 5  // Second
	serverKeepAliveDuration  = 10 // Second

	validationResultAliveDuration = 60 // Second

	masterName = "/master/%s"
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

	// set username and password
	userName := os.Getenv("ETCD_USERNAME")
	password := os.Getenv("ETCD_PASSWORD")
	if len(userName) > 0 {
		config.Username = userName
	}
	if len(password) > 0 {
		config.Password = password
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
func (c *Client) ServerRegister(t context.Context, serverID, nodeType, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	serverKey := fmt.Sprintf("/%s/%s", nodeType, serverID)

	// get a lease
	leaseRsp, err := c.cli.Grant(ctx, serverKeepAliveDuration)
	if err != nil {
		return xerrors.Errorf("Grant lease err:%s", err.Error())
	}

	leaseID := leaseRsp.ID

	txn := c.cli.Txn(context.Background())
	resp, err := txn.
		If(clientv3.Compare(clientv3.CreateRevision(serverKey), "=", 0)).
		Then(clientv3.OpPut(serverKey, value, clientv3.WithLease(leaseID))).
		Else().
		Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return xerrors.Errorf("key already exists")
	}

	// KeepAlive
	keepRespChan, err := c.cli.KeepAlive(context.TODO(), leaseID)
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
func (c *Client) WatchServers(ctx context.Context, nodeType string) clientv3.WatchChan {
	prefix := fmt.Sprintf("/%s/", nodeType)

	watcher := clientv3.NewWatcher(c.cli)
	watchRespChan := watcher.Watch(ctx, prefix, clientv3.WithPrefix())

	return watchRespChan
}

// GetServers get servers
func (c *Client) GetServers(nodeType string) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	serverKeyPrefix := fmt.Sprintf("/%s/", nodeType)
	kv := clientv3.NewKV(c.cli)

	return kv.Get(ctx, serverKeyPrefix, clientv3.WithPrefix())
}

// ServerUnRegister UnRegister to etcd
func (c *Client) ServerUnRegister(t context.Context, serverID, nodeType string) error {
	serverKey := fmt.Sprintf("/%s/%s", nodeType, serverID)
	_, err := c.cli.Delete(context.Background(), serverKey)

	return err
}

// SCUnmarshal  Unmarshal SchedulerCfg
func SCUnmarshal(data []byte, out interface{}) error {
	return json.Unmarshal(data, out)
}

// SCMarshal  Marshal SchedulerCfg
func SCMarshal(s interface{}) ([]byte, error) {
	return json.Marshal(s)
}

func (c *Client) acquireLock(lockPfx string, leaseID clientv3.LeaseID) error {
	s, err := concurrency.NewSession(c.cli, concurrency.WithLease(leaseID))
	if err != nil {
		return err
	}

	m := concurrency.NewMutex(s, lockPfx)

	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	return m.Lock(ctx)
}

func (c *Client) releaseLock(lockPfx string, leaseID clientv3.LeaseID) error {
	s, err := concurrency.NewSession(c.cli, concurrency.WithLease(leaseID))
	if err != nil {
		return err
	}

	m := concurrency.NewMutex(s, lockPfx)
	// need to call the lock function first, then the myKey inside the Mutex object will have a value, and then you can unlock it later.
	err = m.Lock(context.Background())
	if err != nil {
		return err
	}

	return m.Unlock(context.Background())
}

// AcquireMasterLock Request to become a master server
func (c *Client) AcquireMasterLock(serverType string) (clientv3.LeaseID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	lease, err := c.cli.Grant(ctx, validationResultAliveDuration)
	if err != nil {
		return 0, err
	}

	err = c.acquireLock(fmt.Sprintf(masterName, serverType), lease.ID)
	if err != nil {
		return 0, err
	}

	// KeepAlive
	keepRespChan, err := c.cli.KeepAlive(context.TODO(), lease.ID)
	if err != nil {
		return 0, err
	}
	// lease keepalive response queue capacity only 16 , so need to read it
	go func() {
		for {
			<-keepRespChan
		}
	}()

	return lease.ID, nil
}

// ReleaseMasterLock release master lock
func (c *Client) ReleaseMasterLock(leaseID clientv3.LeaseID, serverType string) error {
	return c.releaseLock(fmt.Sprintf(masterName, serverType), leaseID)
}
