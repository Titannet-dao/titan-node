package modules

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"

	"github.com/Filecoin-Titan/titan/build"
	"github.com/Filecoin-Titan/titan/lib/ulimit"
	"github.com/docker/go-units"

	"github.com/Filecoin-Titan/titan/node/modules/dtypes"
	"github.com/Filecoin-Titan/titan/node/repo"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"github.com/Filecoin-Titan/titan/node/scheduler/assets"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/types"
	"github.com/google/uuid"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

const (
	// ServerIDName Server ID key name in the keystore
	ServerIDName = "server-id" //nolint:gosec
	// KTServerIDSecret Key type for server ID secret
	KTServerIDSecret = "server-id-secret" //nolint:gosec
	// PrivateKeyName privateKey key name in the keystore
	PrivateKeyName = "private-key" //nolint:gosec
)

// LockedRepo returns a function that returns the locked repository with an added lifecycle hook to close the repository
func LockedRepo(lr repo.LockedRepo) func(lc fx.Lifecycle) repo.LockedRepo {
	return func(lc fx.Lifecycle) repo.LockedRepo {
		lc.Append(fx.Hook{
			OnStop: func(_ context.Context) error {
				return lr.Close()
			},
		})

		return lr
	}
}

// NewServerID generates and returns the server ID
func NewServerID(lr repo.LockedRepo) (dtypes.ServerID, error) {
	keystore, err := lr.KeyStore()
	if err != nil {
		return "", err
	}

	key, err := keystore.Get(ServerIDName)

	if errors.Is(err, types.ErrKeyInfoNotFound) {
		log.Warn("Generating new server id")

		uid := []byte(uuid.NewString())

		key = types.KeyInfo{
			Type:       KTServerIDSecret,
			PrivateKey: uid,
		}

		if err := keystore.Put(ServerIDName, key); err != nil {
			return "", xerrors.Errorf("writing server id: %w", err)
		}

		if err := lr.SetServerID(uid); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", xerrors.Errorf("could not get server id: %w", err)
	}

	return dtypes.ServerID(key.PrivateKey), nil
}

// NewPrivateKey generates and returns the private key
func NewPrivateKey(lr repo.LockedRepo) (*rsa.PrivateKey, error) {
	keystore, err := lr.KeyStore()
	if err != nil {
		return nil, err
	}

	key, err := keystore.Get(PrivateKeyName)

	if errors.Is(err, types.ErrKeyInfoNotFound) {
		log.Warn("Generating new private key")

		privateKey, err := rsa.GenerateKey(rand.Reader, units.KiB)
		if err != nil {
			return nil, xerrors.Errorf("GenerateKey: %w", err)
		}

		key = types.KeyInfo{
			Type:       PrivateKeyName,
			PrivateKey: titanrsa.PrivateKey2Pem(privateKey),
		}

		if err := keystore.Put(PrivateKeyName, key); err != nil {
			return nil, xerrors.Errorf("writing private key: %w", err)
		}
	} else if err != nil {
		return nil, xerrors.Errorf("could not get private key: %w", err)
	}

	return titanrsa.Pem2PrivateKey(key.PrivateKey)
}

// Datastore returns a new metadata datastore
func Datastore(db *db.SQLDB, serverID dtypes.ServerID) (dtypes.MetadataDS, error) {
	return assets.NewDatastore(db, serverID), nil
}

// CheckFdLimit checks the file descriptor limit and returns an error if the limit is too low
func CheckFdLimit() error {
	limit, _, err := ulimit.GetLimit()
	switch {
	case err == ulimit.ErrUnsupported:
		log.Errorw("checking file descriptor limit failed", "error", err)
	case err != nil:
		return xerrors.Errorf("checking fd limit: %w", err)
	default:
		if limit < build.EdgeFDLimit {
			return xerrors.Errorf("soft file descriptor limit (ulimit -n) too low, want %d, current %d", build.EdgeFDLimit, limit)
		}
	}
	return nil
}
