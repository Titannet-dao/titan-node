package locator

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/fx"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/filecoin-project/go-jsonrpc"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("locator")

const (
	unknownAreaID   = "unknown-unknown"
	maxRandomNumber = 10000
)

type SchedulerAPIMap map[string]*SchedulerAPI

type Storage interface {
	GetSchedulerConfigs(areaID string) ([]*types.SchedulerCfg, error)
	GetAllSchedulerConfigs() []*types.SchedulerCfg
}

type Locator struct {
	fx.In

	*common.CommonAPI
	region.Region
	Storage
	*config.LocatorCfg
	*DNSServer
	Rand          *rand.Rand
	ScheduelrAPIs SchedulerAPIMap
}

type SchedulerAPI struct {
	api.Scheduler
	config *types.SchedulerCfg
}

func isValid(geo string) bool {
	return len(geo) > 0 && !strings.Contains(geo, unknownAreaID)
}

// GetAccessPoints get schedulers urls with special areaID, and those schedulers have the node
func (l *Locator) GetAccessPoints(ctx context.Context, nodeID, areaID string) ([]string, error) {
	if len(nodeID) == 0 || len(areaID) == 0 {
		return nil, fmt.Errorf("params nodeID or areaID can not empty")
	}

	log.Debugf("GetAccessPoints, nodeID %s, areaID %s", nodeID, areaID)

	if schedulers, err := l.getSchedulerWithAreaID(areaID, nodeID); err == nil && len(schedulers) > 0 {
		return schedulers, nil
	}

	log.Debugf("Can not found node %s on %s, will search on all scheduler", nodeID, areaID)

	configs := l.GetAllSchedulerConfigs()
	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return nil, err
	}

	return l.selectBestSchedulers(schedulerAPIs, nodeID)
}

func (l *Locator) getSchedulerWithAreaID(areaID string, nodeID string) ([]string, error) {
	configs, err := l.GetSchedulerConfigs(areaID)
	if err != nil {
		return nil, err
	}

	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return nil, err
	}

	return l.selectBestSchedulers(schedulerAPIs, nodeID)
}

// GetAccessPointsV2 get schedulers urls with special areaID, and those schedulers have the node
func (l *Locator) GetAccessPointsV2(ctx context.Context, nodeID, areaID string) (*types.AccessPointRsp, error) {
	if len(nodeID) == 0 || len(areaID) == 0 {
		return nil, fmt.Errorf("params nodeID or areaID can not empty")
	}

	log.Debugf("GetAccessPoints, nodeID %s, areaID %s", nodeID, areaID)

	configs, err := l.GetSchedulerConfigs(areaID)
	if err != nil {
		return nil, err
	}

	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return nil, err
	}

	schedulers, err := l.selectBestSchedulers(schedulerAPIs, nodeID)
	if err != nil {
		return nil, err
	}

	rsp := &types.AccessPointRsp{Schedulers: schedulers}

	remoteAddr := handler.GetRemoteAddr(ctx)
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return nil, err
	}

	geoInfo, err := l.GetGeoInfo(ip)
	if err != nil {
		log.Errorf("GetGeoInfo error %s", err.Error())
		return rsp, nil
	}

	rsp.GeoInfo = geoInfo
	return rsp, nil
}

func (l *Locator) selectBestSchedulers(apis []*SchedulerAPI, nodeID string) ([]string, error) {
	if len(apis) == 0 {
		return nil, nil
	}

	timeout, err := time.ParseDuration(l.Timeout)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var schedulerConfig *types.SchedulerCfg
	wg := &sync.WaitGroup{}

	for _, api := range apis {
		wg.Add(1)
		go func(ctx context.Context, s *SchedulerAPI) {
			defer wg.Done()

			if err := s.NodeExists(ctx, nodeID); err != nil {
				log.Warnf("check node %s exists %s, sheduler %s", nodeID, err.Error(), s.config.SchedulerURL)
				return
			}

			// cancel()
			schedulerConfig = s.config
		}(ctx, api)
	}

	wg.Wait()

	if schedulerConfig != nil {
		return []string{schedulerConfig.SchedulerURL}, nil
	}
	return []string{}, nil
}

func (l *Locator) randomSchedulerConfigWithWeight(configs []*types.SchedulerCfg) []string {
	totalWeight := 0
	for _, config := range configs {
		totalWeight += config.Weight
	}

	sort.Slice(configs, func(i, j int) bool {
		return configs[i].SchedulerURL < configs[j].SchedulerURL
	})

	randomWeight := float32(l.Rand.Intn(maxRandomNumber)) / float32(maxRandomNumber)
	preAddWeight := float32(0)

	for _, config := range configs {
		nextAddWeight := preAddWeight + float32(config.Weight)
		preWeight := preAddWeight / float32(totalWeight)
		nextWeight := nextAddWeight / float32(totalWeight)

		if randomWeight > preWeight && randomWeight <= nextWeight {
			return []string{config.SchedulerURL}
		}

		preAddWeight = nextAddWeight
	}

	return nil
}

func (l *Locator) getOrNewSchedulerAPIs(configs []*types.SchedulerCfg) ([]*SchedulerAPI, error) {
	schedulerAPIs := make([]*SchedulerAPI, 0, len(configs))

	for _, config := range configs {
		api, err := l.getOrNewSchedulerAPI(config)
		if err != nil {
			log.Errorf("new scheduler api error %s", err.Error())
			continue
		}

		schedulerAPIs = append(schedulerAPIs, api)
	}
	return schedulerAPIs, nil
}

func (l *Locator) getOrNewSchedulerAPI(config *types.SchedulerCfg) (*SchedulerAPI, error) {
	if l.ScheduelrAPIs == nil {
		l.ScheduelrAPIs = make(map[string]*SchedulerAPI)
	}

	scheduler, ok := l.ScheduelrAPIs[config.SchedulerURL]
	if ok {
		return scheduler, nil
	}

	log.Debugf("newSchedulerAPI, url:%s, areaID:%s, accessToken:%s", config.SchedulerURL, config.AreaID, config.AccessToken)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+config.AccessToken)
	api, _, err := client.NewScheduler(context.Background(), config.SchedulerURL, headers, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
	if err != nil {
		return nil, err
	}

	scheduler = &SchedulerAPI{api, config}
	l.ScheduelrAPIs[config.SchedulerURL] = scheduler

	return scheduler, nil
}

func (l *Locator) EdgeDownloadInfos(ctx context.Context, cid string) ([]*types.EdgeDownloadInfoList, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	areaID, err := l.getAreaID(remoteAddr)
	if err != nil {
		return nil, err
	}

	configs, err := l.GetSchedulerConfigs(areaID)
	if err != nil {
		return nil, err
	}

	if len(configs) == 0 {
		configs = l.GetAllSchedulerConfigs()
	}

	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return nil, err
	}

	if len(schedulerAPIs) == 0 {
		return nil, fmt.Errorf("area %s no scheduler exist", areaID)
	}

	log.Debugf("EdgeDownloadInfos, schedulerAPIs %#v", schedulerAPIs)

	// TODO limit concurrency
	return l.getEdgeDownloadInfoFromBestScheduler(schedulerAPIs, cid)
}

// getAreaID get areaID from remote address
func (l *Locator) getAreaID(remoteAddr string) (string, error) {
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return "", err
	}

	geoInfo, err := l.GetGeoInfo(ip)
	if err != nil {
		log.Errorf("getAreaID error %s", err.Error())
		return "", err
	}

	if geoInfo != nil && isValid(geoInfo.Geo) {
		return geoInfo.Geo, nil
	}

	return "", fmt.Errorf("can not get AreaID for %s", ip)
}

func (l *Locator) getEdgeDownloadInfoFromBestScheduler(apis []*SchedulerAPI, cid string) ([]*types.EdgeDownloadInfoList, error) {
	if len(apis) == 0 {
		return nil, fmt.Errorf("scheduler api is empty")
	}

	timeout, err := time.ParseDuration(l.Timeout)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	infoListCh := make(chan *types.EdgeDownloadInfoList)
	errCh := make(chan error)

	for _, api := range apis {
		go func(ctx context.Context, s *SchedulerAPI, ch chan *types.EdgeDownloadInfoList, errCh chan error) {
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
			if infosList != nil {
				results = append(results, infosList)
			}
		case err := <-errCh:
			log.Debugf("get edge download infos error:%s", err.Error())
			resultCount++
		}

		if resultCount == len(apis) {
			return results, nil
		}
	}
}

func (l *Locator) CandidateDownloadInfos(ctx context.Context, cid string) ([]*types.CandidateDownloadInfo, error) {
	configs := l.GetAllSchedulerConfigs()

	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return nil, err
	}

	if len(schedulerAPIs) == 0 {
		return nil, fmt.Errorf("CandidateDownloadInfos no scheduler exist")
	}

	downloadInfos, err := l.GetAssetSourceDownloadInfos(ctx, cid)
	if err != nil {
		return nil, err
	}

	candidateDownloadInfos := make([]*types.CandidateDownloadInfo, 0)
	for _, downloadInfo := range downloadInfos {
		for _, source := range downloadInfo.SourceList {

			if !strings.HasPrefix(source.NodeID, "c_") || len(source.Address) == 0 {
				continue
			}

			candidateDownloadInfo := &types.CandidateDownloadInfo{NodeID: source.NodeID, Address: source.Address}
			candidateDownloadInfos = append(candidateDownloadInfos, candidateDownloadInfo)
		}
	}

	return candidateDownloadInfos, nil
}

// GetAssetSourceDownloadInfo
func (l *Locator) GetAssetSourceDownloadInfos(ctx context.Context, cid string) ([]*types.AssetSourceDownloadInfoRsp, error) {
	configs := l.GetAllSchedulerConfigs()

	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return nil, err
	}

	if len(schedulerAPIs) == 0 {
		return nil, fmt.Errorf("GetAssetSourceDownloadInfos no scheduler exist")
	}

	timeout, err := time.ParseDuration(l.Timeout)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	downloadInfos := make([]*types.AssetSourceDownloadInfoRsp, 0)
	lock := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for _, api := range schedulerAPIs {
		wg.Add(1)

		go func(ctx context.Context, s *SchedulerAPI) {
			defer wg.Done()

			if info, err := s.GetAssetSourceDownloadInfo(ctx, cid); err == nil {
				if len(info.SourceList) > 0 {
					info.SchedulerURL = s.config.SchedulerURL

					lock.Lock()
					downloadInfos = append(downloadInfos, info)
					lock.Unlock()
				}
			} else {
				log.Errorf("GetCandidateDownloadInfos cid %s scheduler %s, error: %s", cid, s.config.SchedulerURL, err.Error())
			}
		}(ctx, api)
	}

	wg.Wait()
	return downloadInfos, nil
}

// GetUserAccessPoint get user access point for special user ip
func (l *Locator) GetUserAccessPoint(ctx context.Context, userIP string) (*api.AccessPoint, error) {
	if len(userIP) == 0 {
		remoteAddr := handler.GetRemoteAddr(ctx)
		host, _, err := net.SplitHostPort(remoteAddr)
		if err != nil {
			return nil, err
		}

		userIP = host
	}

	geoInfo, err := l.GetGeoInfo(userIP)
	if err != nil {
		return nil, err
	}

	configs := l.GetAllSchedulerConfigs()
	if geoInfo.Longitude == 0 && geoInfo.Latitude == 0 {
		schedulers := l.getSchedulerWithDefaultArea(configs)
		if len(schedulers) == 0 {
			return nil, fmt.Errorf("Can not get scheduler for user %s, default areas %#v", userIP, l.DefaultAreas)
		}
		return &api.AccessPoint{AreaID: geoInfo.Geo, SchedulerURLs: []string{schedulers[0].SchedulerURL}}, nil
	}

	log.Infof("GetUserAccessPoint ip %s, areaID %s", userIP, geoInfo.Geo)

	if cfg, err := l.getSchedulerWithUserLastEntry(configs, userIP); err != nil {
		return nil, err
	} else if cfg != nil {
		log.Debugf("getSchedulerWithUserLastEntry %s %s", cfg.SchedulerURL, userIP)
		return &api.AccessPoint{AreaID: geoInfo.Geo, SchedulerURLs: []string{cfg.SchedulerURL}}, nil
	}

	schedulers := l.getSchedulerWithGeoInfo(geoInfo, configs)
	if len(schedulers) == 0 {
		schedulers = l.getSchedulerWithDefaultArea(configs)
	}

	scheduler := l.getSchedulerByLowestNumberOfNodes(schedulers)
	if scheduler != nil {
		return &api.AccessPoint{AreaID: geoInfo.Geo, SchedulerURLs: []string{scheduler.SchedulerURL}}, nil
	}
	return &api.AccessPoint{AreaID: geoInfo.Geo, SchedulerURLs: make([]string, 0)}, nil
}

// Filtering schedulers in the same region
func (l *Locator) getSchedulerWithGeoInfo(geoInfo *region.GeoInfo, schedulerCfgs []*types.SchedulerCfg) []*types.SchedulerCfg {
	sameCity := make([]*types.SchedulerCfg, 0)
	sameProvince := make([]*types.SchedulerCfg, 0)
	sameCountry := make([]*types.SchedulerCfg, 0)

	for _, cfg := range schedulerCfgs {
		_, country, province, city := region.DecodeAreaID(cfg.AreaID)
		if strings.ToLower(geoInfo.Country) != strings.ToLower(country) {
			continue
		}

		if strings.ToLower(province) == strings.ToLower(geoInfo.Province) &&
			strings.ToLower(city) == strings.ToLower(geoInfo.City) {
			sameCity = append(sameCity, cfg)
			continue
		}

		if strings.ToLower(province) == strings.ToLower(geoInfo.Province) {
			sameProvince = append(sameProvince, cfg)
			continue
		}

		sameCountry = append(sameCountry, cfg)
	}

	if len(sameCity) > 0 {
		return sameCity
	}

	if len(sameProvince) > 0 {
		return sameProvince
	}

	return sameCountry
}

func (l *Locator) getSchedulerWithDefaultArea(schedulerCfgs []*types.SchedulerCfg) []*types.SchedulerCfg {
	defaultAreas := make(map[string]struct{})
	for _, area := range l.DefaultAreas {
		defaultAreas[area] = struct{}{}
	}

	cfgs := make([]*types.SchedulerCfg, 0)
	for _, schedulerCfg := range schedulerCfgs {
		if _, ok := defaultAreas[schedulerCfg.AreaID]; ok {
			cfgs = append(cfgs, schedulerCfg)
		}
	}

	return cfgs
}

// Get the scheduler with the lowest number of nodes
func (l *Locator) getSchedulerByLowestNumberOfNodes(schedulerCfgs []*types.SchedulerCfg) *types.SchedulerCfg {
	if len(schedulerCfgs) == 0 {
		return nil
	}
	if len(schedulerCfgs) == 1 {
		return schedulerCfgs[0]
	}

	schedulerAPIs, err := l.getOrNewSchedulerAPIs(schedulerCfgs)
	if err != nil {
		return nil
	}

	lock := sync.Mutex{}
	wg := &sync.WaitGroup{}
	var config *types.SchedulerCfg
	minCount := math.MaxInt
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, s := range schedulerAPIs {
		wg.Add(1)

		go func(schedulerAPI *SchedulerAPI) {
			defer wg.Done()
			count, err := schedulerAPI.GetOnlineNodeCount(ctx, types.NodeEdge)
			if err != nil {
				log.Errorf("getSchedulerByLowestNumberOfNodes GetOnlineNodeCount %s", err.Error())
				return
			}

			lock.Lock()
			if count < minCount {
				minCount = count
				config = schedulerAPI.config
			}
			lock.Unlock()
		}(s)
	}

	wg.Wait()

	return config
}

func (l *Locator) getSchedulerWithUserLastEntry(configs []*types.SchedulerCfg, userIP string) (*types.SchedulerCfg, error) {
	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return nil, err
	}

	var config *types.SchedulerCfg
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	for _, s := range schedulerAPIs {
		wg.Add(1)

		go func(schedulerAPI *SchedulerAPI) {
			defer wg.Done()
			if ok, err := schedulerAPI.CheckIpUsage(ctx, userIP); err != nil {
				return
			} else if ok {
				config = schedulerAPI.config
				cancel()
			}
		}(s)
	}

	wg.Wait()

	return config, nil
}

// GetCandidateIP retrieves ip of candidate
func (l *Locator) GetCandidateIP(ctx context.Context, nodeID string) (string, error) {
	configs := l.GetAllSchedulerConfigs()
	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return "", err
	}

	if len(schedulerAPIs) == 0 {
		return "", fmt.Errorf("no scheduler exist")
	}

	timeout, err := time.ParseDuration(l.Timeout)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// TODO limit concurrency
	var candidateIP string
	wg := &sync.WaitGroup{}
	for _, api := range schedulerAPIs {
		wg.Add(1)

		go func(ctx context.Context, s *SchedulerAPI) {
			defer wg.Done()

			if ip, err := s.GetCandidateNodeIP(ctx, nodeID); err == nil {
				candidateIP = ip
				cancel()
			} else {
				log.Debugf("GetCandidateNodeIP %s", err.Error())
			}
		}(ctx, api)
	}
	wg.Wait()

	if len(candidateIP) == 0 {
		return "", fmt.Errorf("can not get candidate %s ip", nodeID)
	}

	return candidateIP, nil
}

// GetSchedulerWithNode get the scheduler that the node is already connected to
func (l *Locator) GetSchedulerWithNode(ctx context.Context, nodeID string) (string, error) {
	configs := l.GetAllSchedulerConfigs()
	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return "", err
	}

	if len(schedulerAPIs) == 0 {
		return "", fmt.Errorf("no scheduler exist")
	}

	timeout, err := time.ParseDuration(l.Timeout)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// TODO limit concurrency
	var schedulerURL string
	wg := &sync.WaitGroup{}
	for _, api := range schedulerAPIs {
		wg.Add(1)

		go func(ctx context.Context, s *SchedulerAPI) {
			defer wg.Done()

			if _, err := s.GetCandidateNodeIP(ctx, nodeID); err == nil {
				schedulerURL = s.config.SchedulerURL
				cancel()
			} else {
				log.Debugf("GetCandidateNodeIP %s", err.Error())
			}
		}(ctx, api)
	}
	wg.Wait()

	return schedulerURL, nil
}

// func (l *Locator) GetSchedulerWithAPIKey(ctx context.Context, apiKey string) (string, error) {
// 	configs := l.GetAllSchedulerConfigs()
// 	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
// 	if err != nil {
// 		return "", err
// 	}

// 	if len(schedulerAPIs) == 0 {
// 		return "", fmt.Errorf("no scheduler exist")
// 	}

// 	timeout, err := time.ParseDuration(l.Timeout)
// 	if err != nil {
// 		return "", err
// 	}

// 	ctx, cancel := context.WithTimeout(ctx, timeout*time.Second)
// 	defer cancel()

// 	// TODO limit concurrency
// 	var schedulerURL string
// 	wg := &sync.WaitGroup{}
// 	for _, api := range schedulerAPIs {
// 		wg.Add(1)

// 		go func(ctx context.Context, s *SchedulerAPI) {
// 			defer wg.Done()

// 			payload, err := s.AuthVerify(ctx, apiKey)
// 			if err != nil {
// 				log.Debugf("AuthVerify %s", err.Error())
// 				return
// 			}

// 			keys, err := s.GetAPIKeys(context.TODO(), payload.ID)
// 			if err != nil {
// 				log.Debugf("AuthVerify %s", err.Error())
// 				return
// 			}

// 			for _, key := range keys {
// 				if key.APIKey == apiKey {
// 					schedulerURL = s.config.SchedulerURL
// 					cancel()
// 					break
// 				}
// 			}
// 		}(ctx, api)
// 	}
// 	wg.Wait()

// 	return schedulerURL, nil
// }

func (l *Locator) AllocateSchedulerForNode(ctx context.Context, nodeType types.NodeType, code string) (string, error) {
	configs := l.GetAllSchedulerConfigs()
	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return "", err
	}

	if len(schedulerAPIs) == 0 {
		return "", fmt.Errorf("no scheduler exist")
	}

	timeout, err := time.ParseDuration(l.Timeout)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var scheduler string
	wg := &sync.WaitGroup{}
	for _, api := range schedulerAPIs {
		wg.Add(1)

		go func(ctx context.Context, s *SchedulerAPI) {
			defer wg.Done()

			if ok, err := s.CandidateCodeExist(ctx, code); err == nil {
				if ok {
					scheduler = s.config.SchedulerURL
					cancel()
				}
			} else {
				log.Debugf("CandidateCodeExist %s", err.Error())
			}
		}(ctx, api)
	}
	wg.Wait()

	if len(scheduler) == 0 {
		return "", fmt.Errorf("code %s not exist any scheduler", code)
	}

	return scheduler, nil
}

func convertAreasToMap(areas []string) map[string]struct{} {
	areaMap := make(map[string]struct{})
	for _, area := range areas {
		areaMap[area] = struct{}{}
	}

	return areaMap
}

// GetDeploymentCandidateIP retrieves candidate ip of deployment
func (l *Locator) GetDeploymentCandidateIP(ctx context.Context, deploymentID string) (string, error) {
	configs := l.GetAllSchedulerConfigs()
	schedulerAPIs, err := l.getOrNewSchedulerAPIs(configs)
	if err != nil {
		return "", err
	}

	if len(schedulerAPIs) == 0 {
		return "", fmt.Errorf("no scheduler exist")
	}

	timeout, err := time.ParseDuration(l.Timeout)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// TODO limit concurrency
	var candidateIP string
	wg := &sync.WaitGroup{}
	for _, api := range schedulerAPIs {
		wg.Add(1)

		go func(ctx context.Context, s *SchedulerAPI) {
			defer wg.Done()

			if ip, err := s.GetDeploymentProviderIP(ctx, types.DeploymentID(deploymentID)); err == nil {
				candidateIP = ip
				cancel()
			} else {
				log.Debugf("GetDeploymentProviderIP %s", err.Error())
			}
		}(ctx, api)
	}
	wg.Wait()

	if len(candidateIP) == 0 {
		return "", fmt.Errorf("can not get candidate %s ip", deploymentID)
	}

	return candidateIP, nil
}
