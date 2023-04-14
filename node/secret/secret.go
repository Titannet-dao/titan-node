package secret

import (
	"crypto/rand"
	"errors"
	"io"
	"io/ioutil"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/repo"
	"github.com/Filecoin-Titan/titan/node/types"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gbrlsnchs/jwt/v3"
	logging "github.com/ipfs/go-log/v2"

	"golang.org/x/xerrors"
)

const (
	// JWTSecretName represents the name of the JWT private key in the keystore.
	JWTSecretName = "auth-jwt-private"
	// KTJwtHmacSecret represents the key type of the JWT HMAC secret.
	KTJwtHmacSecret = "jwt-hmac-secret"
)

// JwtPayload represents the payload of a JWT token.
type JwtPayload struct {
	Allow []auth.Permission
}

var log = logging.Logger("jwt")

// APISecret retrieves or generates a new HMACSHA JWT secret from the repository's keystore.
// It returns the JWT HMACSHA secret and an error if any.
func APISecret(lr repo.LockedRepo) (*jwt.HMACSHA, error) {
	keystore, err := lr.KeyStore()
	if err != nil {
		return nil, err
	}

	key, err := keystore.Get(JWTSecretName)

	if errors.Is(err, types.ErrKeyInfoNotFound) {
		log.Warn("Generating new API secret")

		sk, err := ioutil.ReadAll(io.LimitReader(rand.Reader, 32))
		if err != nil {
			return nil, err
		}

		key = types.KeyInfo{
			Type:       KTJwtHmacSecret,
			PrivateKey: sk,
		}

		if err := keystore.Put(JWTSecretName, key); err != nil {
			return nil, xerrors.Errorf("writing API secret: %w", err)
		}

		// TODO: make this configurable
		p := JwtPayload{
			Allow: api.AllPermissions,
		}

		cliToken, err := jwt.Sign(&p, jwt.NewHS256(key.PrivateKey))
		if err != nil {
			return nil, err
		}

		if err := lr.SetAPIToken(cliToken); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, xerrors.Errorf("could not get JWT Token: %w", err)
	}

	return jwt.NewHS256(key.PrivateKey), nil
}
