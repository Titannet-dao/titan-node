package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ipfs/go-cid"
)

func (hs *HttpServer) serveCidList(w http.ResponseWriter, r *http.Request, rootCID string) (int, error) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	root, err := cid.Decode(rootCID)
	if err != nil {
		log.Debugw("unable to decode baseCid", "error", err)
		return http.StatusBadRequest, fmt.Errorf("decode root cid %s error: %s", rootCID, err.Error())
	}

	list, err := hs.asset.ListBlocks(ctx, root)
	if err != nil {
		if err == context.Canceled {
			return http.StatusRequestTimeout, fmt.Errorf("request timeout")
		}
		log.Debugw("unable to list blocks", "error", err)
		return http.StatusInternalServerError, fmt.Errorf("error listing blocks: %s", err)
	}

	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(list)
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("error writing list to response: %s", err)
	}

	log.Debugw("successfully listed blocks", "rootCID", rootCID)
	return http.StatusOK, nil
}
