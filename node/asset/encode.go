package asset

import (
	"bytes"
	"encoding/gob"

	"github.com/Filecoin-Titan/titan/api/types"
)

// AssetPullerEncoder encodes or decodes assetPuller
type AssetPullerEncoder struct {
	Root                    string
	BlocksWaitList          []string
	BlocksPulledSuccessList []string
	NextLayerCIDs           []string
	DownloadSources         []*types.CandidateDownloadInfo
	TotalSize               uint64
	DoneSize                uint64
}

// Encode encodes the input value into a byte slice using gob encoding.
func encode(input interface{}) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(input)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// Decode decodes the input byte slice into the output value using gob decoding.
func decode(data []byte, out interface{}) error {
	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)
	err := dec.Decode(out)
	if err != nil {
		return err
	}
	return nil
}
