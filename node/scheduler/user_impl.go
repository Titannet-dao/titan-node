package scheduler

import (
	"context"
	"fmt"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/scheduler/user"
)

// UserExists checks if the user exists.
func (s *Scheduler) UserAPIKeysExists(ctx context.Context, userID string) error {
	keys, err := s.GetAPIKeys(ctx, userID)
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		return fmt.Errorf("user %s api keys not exist", userID)
	}

	return nil
}

// AllocateStorage allocates storage space.
func (s *Scheduler) AllocateStorage(ctx context.Context, userID string) (*types.StorageSize, error) {
	u := s.newUser(userID)
	return u.AllocateStorage(ctx, s.SchedulerCfg.UserFreeStorageSize)
}

// GetStorageSize get size of user storage
func (s *Scheduler) GetStorageSize(ctx context.Context, userID string) (*types.StorageSize, error) {
	u := s.newUser(userID)
	return u.GetStorageSize(ctx)
}

// CreateAPIKey creates a key for the client API.
func (s *Scheduler) CreateAPIKey(ctx context.Context, userID, keyName string) (string, error) {
	u := s.newUser(userID)
	return u.CreateAPIKey(ctx, keyName, s.CommonAPI)
}

// GetAPIKeys get all api key for user.
func (s *Scheduler) GetAPIKeys(ctx context.Context, userID string) (map[string]string, error) {
	u := s.newUser(userID)
	return u.GetAPIKeys(ctx)
}

func (s *Scheduler) DeleteAPIKey(ctx context.Context, userID, name string) error {
	u := s.newUser(userID)
	return u.DeleteAPIKey(ctx, name)
}

func (s *Scheduler) newUser(userID string) *user.User {
	return &user.User{ID: userID, SQLDB: s.AssetManager.SQLDB, Manager: s.AssetManager}
}