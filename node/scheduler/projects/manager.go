package projects

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/scheduler/node"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/filecoin-project/go-statemachine"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("projects")

const (
	// If the node does not reply more than once, the project timeout is determined.
	projectTimeoutLimit = 3
	// Interval to get asset pull progress from node (Unit:Second)
	progressInterval = 30 * time.Second

	checkFailedProjectInterval    = 5 * time.Minute
	checkServicingProjectInterval = 10 * time.Minute

	maxNodeOfflineTime = 12 * time.Hour

	edgeReplicasLimit = 1000
)

// Manager manages project replicas
type Manager struct {
	*db.SQLDB
	nodeMgr              *node.Manager // node manager
	stateMachineWait     sync.WaitGroup
	projectStateMachines *statemachine.StateGroup
	deployingProjects    sync.Map // Assignments where projects are being pulled
}

// NewManager returns a new projectManager instance
func NewManager(nodeManager *node.Manager, sdb *db.SQLDB, ds datastore.Batching) *Manager {
	m := &Manager{
		SQLDB:   sdb,
		nodeMgr: nodeManager,
	}

	// state machine initialization
	m.stateMachineWait.Add(1)
	m.projectStateMachines = statemachine.New(ds, m, ProjectInfo{})

	return m
}

// Start initializes and starts the project state machine and associated tickers
func (m *Manager) StartTimer(ctx context.Context) {
	if err := m.initStateMachines(); err != nil {
		log.Errorf("restartStateMachines err: %s", err.Error())
	}

	go m.startCheckDeployProgressesTimer()
	go m.startCheckFailedProjectTimer()
	go m.startCheckServicingProjectTimer()
}

type deployingProjectsInfo struct {
	count      int
	expiration time.Time
}

// Reset the count of no response project tasks
func (m *Manager) startProjectTimeoutCounting(id string, count int) {
	info := &deployingProjectsInfo{count: 0}

	infoI, _ := m.deployingProjects.Load(id)
	if infoI != nil {
		info = infoI.(*deployingProjectsInfo)
	} else {
		needTime := int64(60 * 30)
		info.expiration = time.Now().Add(time.Second * time.Duration(needTime))
	}
	info.count = count

	m.deployingProjects.Store(id, info)
}

func (m *Manager) stopProjectTimeoutCounting(id string) {
	m.deployingProjects.Delete(id)
}

func (m *Manager) isProjectTaskExist(id string) bool {
	_, exist := m.deployingProjects.Load(id)

	return exist
}

// removeReplica remove a replica for node
func (m *Manager) removeReplica(id, nodeID string, event types.ProjectEvent) error {
	err := m.DeleteProjectReplica(id, nodeID, event)
	if err != nil {
		return err
	}

	node := m.nodeMgr.GetNode(nodeID)
	if node != nil {
		go node.Delete(context.Background(), id)
	}

	return nil
}

// Terminate stops the project state machine
func (m *Manager) Terminate(ctx context.Context) error {
	log.Infof("Terminate stop")
	return m.projectStateMachines.Stop(ctx)
}

func (m *Manager) retrieveNodeDeployProgresses() {
	deployingNodes := make(map[string][]string)

	m.deployingProjects.Range(func(key, value interface{}) bool {
		id := key.(string)
		info := value.(*deployingProjectsInfo)

		stateInfo, err := m.LoadProjectStateInfo(id, m.nodeMgr.ServerID)
		if err != nil {
			return true
		}

		if stateInfo.State != Deploying.String() {
			return true
		}

		if info.count >= projectTimeoutLimit {
			m.setProjectTimeout(id, fmt.Sprintf("count:%d", info.count))
			return true
		}

		if info.expiration.Before(time.Now()) {
			m.setProjectTimeout(id, fmt.Sprintf("expiration:%s", info.expiration.String()))
			return true
		}

		m.startProjectTimeoutCounting(id, info.count+1)

		nodes, err := m.LoadNodesOfStartingReplica(id)
		if err != nil {
			log.Errorf("retrieveNodeDeployProgresses %s LoadReplicas err:%s", id, err.Error())
			return true
		}

		if len(nodes) > 0 {
			for _, nodeID := range nodes {
				list := deployingNodes[nodeID]
				deployingNodes[nodeID] = append(list, id)
			}
		} else {
			err := m.projectStateMachines.Send(ProjectID(id), DeployResult{})
			if err != nil {
				log.Errorf("retrieveNodeDeployProgresses %s  statemachine send err:%s", id, err.Error())
			}
		}

		return true
	})

	getCP := func(nodeID string, cids []string, delay int) {
		time.Sleep(time.Duration(delay) * time.Second)

		// request node
		result, err := m.requestNodeDeployProgresses(nodeID, cids)
		if err != nil {
			log.Errorf("retrieveNodeDeployProgresses %s requestNodeDeployProgresses err:%s", nodeID, err.Error())
			return
		}

		// update project info
		m.updateProjectDeployResults(nodeID, result)
	}

	duration := 1
	delay := 0
	for nodeID, ids := range deployingNodes {
		delay += duration
		if delay > 50 {
			delay = 0
		}

		go getCP(nodeID, ids, delay)
	}
}

// updateProjectDeployResults updates project results
func (m *Manager) updateProjectDeployResults(nodeID string, result []*types.Project) {
	for _, progress := range result {
		log.Infof("updateProjectDeployResults node_id: %s, status: %d, id: %s msg:%s", nodeID, progress.Status, progress.ID, progress.Msg)

		exist, _ := m.projectStateMachines.Has(ProjectID(progress.ID))
		if !exist {
			continue
		}

		exist = m.isProjectTaskExist(progress.ID)
		if !exist {
			continue
		}

		m.startProjectTimeoutCounting(progress.ID, 0)

		if progress.Status == types.ProjectReplicaStatusStarting {
			continue
		}

		err := m.SaveProjectReplicasInfo(&types.ProjectReplicas{
			Id:     progress.ID,
			NodeID: nodeID,
			Status: progress.Status,
		})
		if err != nil {
			log.Errorf("updateProjectDeployResults %s SaveProjectReplicasInfo err:%s", nodeID, err.Error())
			continue
		}

		err = m.projectStateMachines.Send(ProjectID(progress.ID), DeployResult{})
		if err != nil {
			log.Errorf("updateProjectDeployResults %s %s statemachine send err:%s", nodeID, progress.ID, err.Error())
			continue
		}
	}
}

func (m *Manager) requestNodeDeployProgresses(nodeID string, ids []string) (result []*types.Project, err error) {
	node := m.nodeMgr.GetNode(nodeID)
	if node == nil {
		err = xerrors.Errorf("node %s not found", nodeID)
		return
	}

	result, err = node.Query(context.Background(), ids)
	return
}

func (m *Manager) setProjectTimeout(id, msg string) {
	nodes, err := m.LoadNodesOfStartingReplica(id)
	if err != nil {
		log.Errorf("setProjectTimeout %s LoadNodesOfStartingReplica err:%s", id, err.Error())
		return
	}

	// update replicas status
	err = m.UpdateProjectReplicasStatusToFailed(id)
	if err != nil {
		log.Errorf("setProjectTimeout %s UpdateProjectReplicasStatusToFailed err:%s", id, err.Error())
		return
	}

	for _, nodeID := range nodes {
		node := m.nodeMgr.GetNode(nodeID)
		if node != nil {
			go node.Delete(context.Background(), id)
		}
	}

	err = m.projectStateMachines.Send(ProjectID(id), DeployFailed{error: xerrors.Errorf("deploy timeout ; %s", msg)})
	if err != nil {
		log.Errorf("setProjectTimeout %s send time out err:%s", id, err.Error())
	}
}

// startCheckDeployProgressesTimer Periodically gets asset pull progress
func (m *Manager) startCheckDeployProgressesTimer() {
	ticker := time.NewTicker(progressInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.retrieveNodeDeployProgresses()
	}
}

// startCheckFailedProjectTimer Periodically Check for expired projects,
func (m *Manager) startCheckFailedProjectTimer() {
	ticker := time.NewTicker(checkFailedProjectInterval)
	defer ticker.Stop()

	limit := 10
	offset := 0
	for {
		<-ticker.C

		offset = m.checkUnDoneProjects(limit, offset)
	}
}

// startCheckServicingProjectTimer Periodically Check for expired projects,
func (m *Manager) startCheckServicingProjectTimer() {
	ticker := time.NewTicker(checkServicingProjectInterval)
	defer ticker.Stop()

	limit := 10
	offset := 0
	for {
		<-ticker.C

		offset = m.checkProjectReplicas(limit, offset)
	}
}

func (m *Manager) CheckProjectReplicasFromNode(nodeID string) {
	list, err := m.LoadProjectReplicasForNode(nodeID)
	if err != nil {
		log.Errorf("checkProjectReplicasFromNode projects :%s", err.Error())
		return
	}

	if len(list) == 0 {
		return
	}

	nodeProjects := make([]string, 0)

	// loading projects to local
	for _, info := range list {
		if info.Status == types.ProjectReplicaStatusError {
			continue
		}

		nodeProjects = append(nodeProjects, info.Id)
	}

	if len(nodeProjects) == 0 {
		return
	}

	results, err := m.requestNodeDeployProgresses(nodeID, nodeProjects)
	if err != nil {
		err = m.UpdateProjectReplicaStatusFromNode(nodeID, nodeProjects, types.ProjectReplicaStatusOffline)
		if err != nil {
			log.Errorf("checkProjectReplicas UpdateProjectReplicaStatusFromNode err: %v", err)
		}
		return
	}

	startedList := make([]string, 0)
	for _, result := range results {
		if result.Status == types.ProjectReplicaStatusStarted {
			startedList = append(startedList, result.ID)
		}
	}

	err = m.UpdateProjectReplicaStatusFromNode(nodeID, startedList, types.ProjectReplicaStatusStarted)
	if err != nil {
		log.Errorf("checkProjectReplicas UpdateProjectReplicaStatusFromNode err: %v", err)
	}
}

func (m *Manager) checkProjectReplicas(limit, offset int) int {
	rows, err := m.LoadAllProjectInfos(m.nodeMgr.ServerID, limit, offset, []string{Servicing.String()})
	if err != nil {
		log.Errorf("checkProjectReplicas projects :%s", err.Error())
		return offset
	}
	defer rows.Close()

	nodeProjects := make(map[string][]string, 0)
	projectDeleteReplicas := make(map[string]int64, 0)

	projectLen := 0
	// loading projects to local
	for rows.Next() {
		projectLen++

		cInfo := &types.ProjectInfo{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("checkProjectReplicas StructScan err: %s", err.Error())
			continue
		}

		if cInfo.Expiration.Before(time.Now()) {
			log.Infof("remove %s Send Expiration: %s, now: %s", cInfo.UUID, cInfo.Expiration.String(), time.Now().String())
			// Expiration
			err = m.projectStateMachines.Send(ProjectID(cInfo.UUID), ProjectForceState{State: Remove, Event: int64(types.ProjectEventExpiration)})
			if err != nil {
				log.Errorf("remove %s Send err: %s", cInfo.UUID, err.Error())
			}
			continue
		}

		cInfo.DetailsList, err = m.LoadProjectReplicaInfos(cInfo.UUID)
		if err != nil {
			log.Errorf("checkProjectReplicas %s load replicas err: %s", cInfo.UUID, err.Error())
			continue
		}

		for _, details := range cInfo.DetailsList {
			if details.Status != types.ProjectReplicaStatusStarted && details.Status != types.ProjectReplicaStatusOffline {
				continue
			}
			nodeProjects[details.NodeID] = append(nodeProjects[details.NodeID], details.Id)
		}
	}

	// check project status from node
	for nodeID, projectIDs := range nodeProjects {
		lastSeen, err := m.LoadNodeLastSeenTime(nodeID)
		if err != nil {
			log.Errorf("checkProjectReplicas LoadLastSeenOfNode err: %s", err.Error())
			continue
		}

		if lastSeen.Add(maxNodeOfflineTime).Before(time.Now()) {
			// delete replicas
			for _, id := range projectIDs {
				projectDeleteReplicas[id] += 0

				m.removeReplica(id, nodeID, types.ProjectEventNodeOffline)
			}
			continue
		}

		results, err := m.requestNodeDeployProgresses(nodeID, projectIDs)
		if err != nil {
			err = m.UpdateProjectReplicaStatusFromNode(nodeID, projectIDs, types.ProjectReplicaStatusOffline)
			if err != nil {
				log.Errorf("checkProjectReplicas UpdateProjectReplicaStatusFromNode err: %v", err)
			}
			continue
		}

		for _, result := range results {
			if result.Status != types.ProjectReplicaStatusStarted {
				// delete replicas
				projectDeleteReplicas[result.ID] += 0

				m.removeReplica(result.ID, nodeID, types.ProjectEventStatusChange)
			}
		}
	}

	m.restartProjects(projectDeleteReplicas)

	offset += projectLen
	if projectLen < limit {
		offset = 0
	}

	return offset
}

func (m *Manager) restartProjects(projectDeleteReplicas map[string]int64) {
	for id, count := range projectDeleteReplicas {
		err := m.UpdateProjectStateInfo(id, NodeSelect.String(), 0, count, m.nodeMgr.ServerID)
		if err != nil {
			log.Errorf("restartProject %s UpdateProjectStateInfo err: %s", id, err.Error())
			continue
		}

		// create project task
		err = m.projectStateMachines.Send(ProjectID(id), ProjectForceState{State: NodeSelect})
		if err != nil {
			log.Errorf("restartProject %s Send err: %s", id, err.Error())
			continue
		}
	}
}

func (m *Manager) checkUnDoneProjects(limit, offset int) int {
	rows, err := m.LoadAllProjectInfos(m.nodeMgr.ServerID, limit, offset, []string{Failed.String(), NodeSelect.String(), Update.String()})
	if err != nil {
		log.Errorf("Load projects :%s", err.Error())
		return offset
	}
	defer rows.Close()

	ids := make([]string, 0)

	projectLen := 0
	// loading projects to local
	for rows.Next() {
		projectLen++

		cInfo := &types.ProjectInfo{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("project StructScan err: %s", err.Error())
			continue
		}

		if cInfo.Expiration.Before(time.Now()) {
			log.Infof("remove %s Send Expiration: %s, now: %s", cInfo.UUID, cInfo.Expiration.String(), time.Now().String())
			// Expiration
			err = m.projectStateMachines.Send(ProjectID(cInfo.UUID), ProjectForceState{State: Remove, Event: int64(types.ProjectEventExpiration)})
			if err != nil {
				log.Errorf("remove %s Send err: %s", cInfo.UUID, err.Error())
			}
			continue
		}

		if m.isProjectTaskExist(cInfo.UUID) {
			continue
		}

		ids = append(ids, cInfo.UUID)
	}

	m.RestartDeployProjects(ids)

	offset += projectLen
	if projectLen < limit {
		offset = 0
	}

	return offset
}

func (m *Manager) chooseNodes(needCount int, filterMap map[string]struct{}, info ProjectInfo) []*node.Node {
	out := make([]*node.Node, 0)
	continent, country, province, city := region.DecodeAreaID(info.AreaID)
	if continent != "" {
		list := m.nodeMgr.GeoMgr.FindNodesFromGeo(continent, country, province, city, types.NodeEdge)
		sort.Slice(list, func(i, j int) bool {
			return list[i].BackProjectTime < list[j].BackProjectTime
		})
		for _, nodeInfo := range list {
			if len(out) >= needCount {
				break
			}

			node := m.checkNode(nodeInfo.NodeID, filterMap, info)
			if node == nil {
				continue
			}

			out = append(out, node)
		}
	} else {
		list := m.nodeMgr.GetRandomEdges(needCount)
		log.Infof("chooseNodes needCount %d ,%v", needCount, list)
		// list := m.nodeMgr.GetAllEdgeNode()
		// sort.Slice(list, func(i, j int) bool {
		// 	return list[i].BackProjectTime < list[j].BackProjectTime
		// })
		for nodeID := range list {
			node := m.checkNode(nodeID, filterMap, info)
			if node == nil {
				continue
			}

			out = append(out, node)
		}
	}

	return out
}

func (m *Manager) checkNode(nodeID string, filterMap map[string]struct{}, info ProjectInfo) *node.Node {
	node := m.nodeMgr.GetEdgeNode(nodeID)
	if node == nil {
		return nil
	}

	if _, exist := filterMap[node.NodeID]; exist {
		return nil
	}

	// if node.CPUCores < int(info.CPUCores) {
	// 	return nil
	// }

	// if node.Memory < float64(info.Memory) {
	// 	return nil
	// }

	return node
}
