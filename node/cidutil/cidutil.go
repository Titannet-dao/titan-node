package cidutil

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// CIDToHash converts a CID string to its corresponding hash string.
func CIDToHash(cidString string) (string, error) {
	cid, err := cid.Decode(cidString)
	if err != nil {
		return "", err
	}

	return cid.Hash().String(), nil
}

// HashToCID converts a hash string to its corresponding CID string.
func HashToCID(hashString string) (string, error) {
	multihash, err := mh.FromHexString(hashString)
	if err != nil {
		return "", err
	}
	cid := cid.NewCidV1(cid.Raw, multihash)
	return cid.String(), nil
}
