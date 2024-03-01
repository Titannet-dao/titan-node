package lotuscli

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"

	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("lotus")

// ChainHead lotus ChainHead api
func ChainHead(url string) (uint64, error) {
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

	b, err := json.Marshal(rsp.Result)
	if err != nil {
		return 0, err
	}

	var ts TipSet
	err = json.Unmarshal(b, &ts)
	if err != nil {
		return 0, err
	}

	return ts.Height(), nil
}

// ChainGetTipSetByHeight lotus ChainGetTipSetByHeight api
func ChainGetTipSetByHeight(url string, height int64) (*TipSet, error) {
	serializedParams := params{
		height, nil,
	}

	req := request{
		Jsonrpc: "2.0",
		Method:  "Filecoin.ChainGetTipSetByHeight",
		Params:  serializedParams,
		ID:      1,
	}

	rsp, err := requestLotus(url, req)
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(rsp.Result)
	if err != nil {
		return nil, err
	}

	var ts TipSet
	err = json.Unmarshal(b, &ts)
	if err != nil {
		return nil, err
	}

	return &ts, nil
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

	body, err := io.ReadAll(resp.Body)
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
