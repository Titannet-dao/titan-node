package tunclient

import (
	"context"
	"sync"

	"github.com/Filecoin-Titan/titan/api"
)

type Service struct {
	ID      string
	Address string
	Port    int
}

type Services struct {
	ctx       context.Context
	scheduler api.Scheduler
	nodeID    string
	services  sync.Map
}

func NewServices(ctx context.Context, schedulerAPI api.Scheduler, nodeID string) *Services {
	s := &Services{ctx: ctx, scheduler: schedulerAPI, nodeID: nodeID, services: sync.Map{}}
	return s
}

func (s *Services) Regiseter(service *Service) {
	log.Debugf("Regiseter servie %s %s %d", service.ID, service.Address, service.Port)
	s.services.Store(service.ID, service)
}

func (s *Services) Remove(service *Service) {
	s.services.Delete(service.ID)
}

func (s *Services) Get(serviceID string) *Service {
	v, ok := s.services.Load(serviceID)
	if ok {
		return v.(*Service)
	}
	return nil
}
