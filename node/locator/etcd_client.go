package locator

import (
	"context"
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/etcdcli"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type EtcdClient struct {
	cli *etcdcli.Client
	// key is areaID, value is array of types.SchedulerCfg pointer
	schedulerConfigs map[string][]*types.SchedulerCfg
	// key is etcd key, value is types.SchedulerCfg pointer
	configMap map[string]*types.SchedulerCfg
}

func NewEtcdClient(addresses []string) (*EtcdClient, error) {
	etcd, err := etcdcli.New(addresses)
	if err != nil {
		return nil, err
	}

	ec := &EtcdClient{
		cli:              etcd,
		schedulerConfigs: make(map[string][]*types.SchedulerCfg),
		configMap:        make(map[string]*types.SchedulerCfg),
	}

	if err := ec.loadSchedulerConfigs(); err != nil {
		return nil, err
	}

	go ec.watch()

	return ec, nil
}

func (ec *EtcdClient) loadSchedulerConfigs() error {
	resp, err := ec.cli.GetServers(types.NodeScheduler)
	if err != nil {
		return err
	}

	schedulerConfigs := make(map[string][]*types.SchedulerCfg)

	for _, kv := range resp.Kvs {
		config, err := etcdcli.SCUnmarshal(kv.Value)
		if err != nil {
			return err
		}

		configs, ok := schedulerConfigs[config.AreaID]
		if !ok {
			configs = make([]*types.SchedulerCfg, 0)
		}
		configs = append(configs, config)

		schedulerConfigs[config.AreaID] = configs
		ec.configMap[string(kv.Key)] = config
	}

	ec.schedulerConfigs = schedulerConfigs
	return nil
}

func (ec *EtcdClient) watch() {
	if ec.schedulerConfigs == nil {
		ec.schedulerConfigs = make(map[string][]*types.SchedulerCfg)
	}

	watchChan := ec.cli.WatchServers(context.Background(), types.NodeScheduler)
	for {
		resp, ok := <-watchChan
		if !ok {
			log.Errorf("close watch chan")
			return
		}

		for _, event := range resp.Events {
			switch event.Type {
			case mvccpb.DELETE:
				if err := ec.onDelete(event.Kv); err != nil {
					log.Errorf("on delete error %s", err.Error())
				}
			case mvccpb.PUT:
				if err := ec.onPut(event.Kv); err != nil {
					log.Errorf("on put error %s", err.Error())
				}
			}

		}
	}

}

func (ec *EtcdClient) onPut(kv *mvccpb.KeyValue) error {
	log.Debugf("onPut key: %s", string(kv.Key))
	config, err := etcdcli.SCUnmarshal(kv.Value)
	if err != nil {
		return err
	}

	configs, ok := ec.schedulerConfigs[config.AreaID]
	if !ok {
		configs = make([]*types.SchedulerCfg, 0)
	}
	configs = append(configs, config)

	ec.schedulerConfigs[config.AreaID] = configs
	ec.configMap[string(kv.Key)] = config
	return nil
}

func (ec *EtcdClient) onDelete(kv *mvccpb.KeyValue) error {
	log.Debugf("onDelete key: %s", string(kv.Key))
	config, ok := ec.configMap[string(kv.Key)]
	if !ok {
		return nil
	}

	configs, ok := ec.schedulerConfigs[config.AreaID]
	if !ok {
		return fmt.Errorf("no config in area %s", config.AreaID)
	}

	// remove config
	for i, cfg := range configs {
		if cfg.SchedulerURL == config.SchedulerURL {
			configs = append(configs[:i], configs[i+1:]...)
			break
		}
	}

	ec.schedulerConfigs[config.AreaID] = configs
	delete(ec.configMap, string(kv.Key))
	return nil
}

func (ec *EtcdClient) GetSchedulerConfigs(areaID string) ([]*types.SchedulerCfg, error) {
	return ec.schedulerConfigs[areaID], nil
}
