package locator

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/fx"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/filecoin-project/go-jsonrpc"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("locator")

const (
	// 3 seconds
	connectTimeout = 3
	defaultAreaID  = "CN-GD-Shenzhen"
	unknownAreaID  = "unknown-unknown-unknown"
)

type Storage interface {
	GetSchedulerConfigs(areaID string) ([]*types.SchedulerCfg, error)
}

type Locator struct {
	fx.In

	*common.CommonAPI
	region.Region
	Storage
}

type schedulerAPI struct {
	api.Scheduler
	close jsonrpc.ClientCloser
	url   string
}

func isValid(geo string) bool {
	return len(geo) > 0 && geo != unknownAreaID
}

// GetAccessPoints get schedulers urls with special areaID, and those schedulers have the node
func (l *Locator) GetAccessPoints(ctx context.Context, nodeID, areaID string) ([]string, error) {
	if len(nodeID) == 0 || len(areaID) == 0 {
		return nil, fmt.Errorf("params nodeID or areaID can not empty")
	}
	log.Debugf("GetAccessPoints, nodeID %s, areaID %s", nodeID, areaID)
	configs, err := l.GetSchedulerConfigs(areaID)
	if err != nil {
		return nil, err
	}
	for _, cfg := range configs {
		log.Debugf("cfg:%#v", *cfg)
	}
	schedulerAPIs, err := l.newSchedulerAPIs(configs)
	if err != nil {
		return nil, err
	}

	return l.selectBestSchedulers(schedulerAPIs, nodeID)
}

func (locator *Locator) selectBestSchedulers(apis []*schedulerAPI, nodeID string) ([]string, error) {
	if len(apis) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout*time.Second)
	defer cancel()

	urlCh := make(chan string)
	errCh := make(chan error)

	for _, api := range apis {
		go func(ctx context.Context, s *schedulerAPI, urlCh chan string, errCh chan error) {
			defer s.close()

			if err := s.NodeExists(ctx, nodeID); err != nil {
				errCh <- err
			} else {
				urlCh <- s.url
			}

		}(ctx, api, urlCh, errCh)
	}

	schedulerURLs := make([]string, 0)
	resultCount := 0

	for {
		select {
		case url := <-urlCh:
			cancel()
			resultCount++
			schedulerURLs = append(schedulerURLs, url)
		case err := <-errCh:
			log.Debugf("check node exists error:%s", err.Error())
			resultCount++
		}

		if resultCount == len(apis) {
			return schedulerURLs, nil
		}
	}
}

func (l *Locator) newSchedulerAPIs(configs []*types.SchedulerCfg) ([]*schedulerAPI, error) {
	schedulerAPIs := make([]*schedulerAPI, 0, len(configs))

	for _, config := range configs {
		api, err := l.newSchedulerAPI(config)
		if err != nil {
			log.Errorf("new scheduler api error %s", err.Error())
			continue
		}

		schedulerAPIs = append(schedulerAPIs, api)
	}
	return schedulerAPIs, nil
}

func (l *Locator) newSchedulerAPI(config *types.SchedulerCfg) (*schedulerAPI, error) {
	log.Debugf("newSchedulerAPI, url:%s, areaID:%s, accessToken:%s", config.SchedulerURL, config.AreaID, config.AccessToken)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+config.AccessToken)
	api, close, err := client.NewScheduler(context.Background(), config.SchedulerURL, headers)
	if err != nil {
		return nil, err
	}

	return &schedulerAPI{api, close, config.SchedulerURL}, nil
}

func (l *Locator) EdgeDownloadInfos(ctx context.Context, cid string) (*types.EdgeDownloadInfoList, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	areaID, err := l.getAreaID(remoteAddr)
	if err != nil {
		return nil, err
	}

	configs, err := l.GetSchedulerConfigs(areaID)
	if err != nil {
		return nil, err
	}

	schedulerAPIs, err := l.newSchedulerAPIs(configs)
	if err != nil {
		return nil, err
	}

	return l.getEdgeDownloadInfoFromBestScheduler(schedulerAPIs, cid)
}

// getAreaID get areaID from remote address
func (locator *Locator) getAreaID(remoteAddr string) (string, error) {
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return "", err
	}

	geoInfo, err := locator.GetGeoInfo(ip)
	if err != nil {
		log.Errorf("getAreaID error %s", err.Error())
		return "", err
	}

	if geoInfo != nil && isValid(geoInfo.Geo) {
		return geoInfo.Geo, nil
	}

	return defaultAreaID, nil
}

func (l *Locator) getEdgeDownloadInfoFromBestScheduler(apis []*schedulerAPI, cid string) (*types.EdgeDownloadInfoList, error) {
	if len(apis) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout*time.Second)
	defer cancel()

	infoListCh := make(chan *types.EdgeDownloadInfoList)
	errCh := make(chan error)

	for _, api := range apis {
		go func(ctx context.Context, s *schedulerAPI, ch chan *types.EdgeDownloadInfoList, errCh chan error) {
			defer s.close()

			if infoList, err := s.GetEdgeDownloadInfos(ctx, cid); err != nil {
				errCh <- err
			} else {
				infoListCh <- infoList
			}

		}(ctx, api, infoListCh, errCh)
	}

	resultCount := 0
	results := make([]*types.EdgeDownloadInfoList, 0)

	for {
		select {
		case infosList := <-infoListCh:
			resultCount++
			results = append(results, infosList)
		case err := <-errCh:
			log.Debugf("get edge download infos error:%s", err.Error())
			resultCount++
		}

		if resultCount == len(apis) {
			if len(results) > 0 {
				return results[0], nil
			}
			return nil, nil
		}
	}
}

// GetUserAccessPoint get user access point for special user ip
func (l *Locator) GetUserAccessPoint(ctx context.Context, userIP string) (*api.AccessPoint, error) {
	areaID := defaultAreaID
	geoInfo, err := l.GetGeoInfo(userIP)
	if err != nil {
		return nil, err
	}

	if geoInfo != nil && isValid(geoInfo.Geo) {
		areaID = geoInfo.Geo
	}

	configs, err := l.GetSchedulerConfigs(areaID)
	if err != nil {
		return nil, err
	}

	schedulerURLs := make([]string, 0, len(configs))
	for _, cfg := range configs {
		schedulerURLs = append(schedulerURLs, cfg.SchedulerURL)
	}

	return &api.AccessPoint{AreaID: areaID, SchedulerURLs: schedulerURLs}, nil
}
