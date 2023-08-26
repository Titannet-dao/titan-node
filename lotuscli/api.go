package lotuscli

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("lotus")

// ChainHead lotus ChainHead api
func ChainHead(url string) (int64, error) {
	req := request{
		Jsonrpc: "2.0",
		Method:  "Filecoin.ChainHead",
		Params:  nil,
		ID:      1,
	}

	rsp, err := requestLotus(url, req)
	if err != nil {
		return 0, err
	}

	var ts tipSet
	b, err := json.Marshal(rsp.Result)
	if err != nil {
		return 0, err
	}

	err = json.Unmarshal(b, &ts)
	if err != nil {
		return 0, err
	}

	return ts.Height, nil
}

// StateGetRandomnessFromBeacon lotus StateGetRandomnessFromBeacon api
func StateGetRandomnessFromBeacon(url string) (int64, error) {
	seed := time.Now().UnixNano()

	height, err := ChainHead(url)
	if err != nil {
		return seed, err
	}

	serializedParams, err := json.Marshal(params{
		0, height, nil, nil,
	})
	if err != nil {
		return seed, err
	}

	req := request{
		Jsonrpc: "2.0",
		Method:  "Filecoin.StateGetRandomnessFromBeacon",
		Params:  serializedParams,
		ID:      1,
	}

	rsp, err := requestLotus(url, req)
	if err != nil {
		return seed, err
	}

	var rs randomness
	b, err := json.Marshal(rsp.Result)
	if err != nil {
		return seed, err
	}
	err = json.Unmarshal(b, &rs)
	if err != nil {
		return seed, err
	}

	if len(rs) >= 3 {
		s := binary.BigEndian.Uint32(rs)
		log.Debugf("lotus Randomness:%d \n", s)
		return int64(s), nil
	}

	log.Debugf("lotus Randomness rs:%v \n", rs)
	return seed, nil
}

func requestLotus(url string, req request) (*response, error) {
	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var rsp response
	err = json.Unmarshal(body, &rsp)
	if err != nil {
		return nil, err
	}

	if rsp.Error != nil {
		return nil, xerrors.New(rsp.Error.Message)
	}

	return &rsp, nil
}
