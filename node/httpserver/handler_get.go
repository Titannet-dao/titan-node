package httpserver

import (
	"bytes"
	"crypto"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"strings"

	"github.com/Filecoin-Titan/titan/api/types"
	titanrsa "github.com/Filecoin-Titan/titan/node/rsa"
	"golang.org/x/xerrors"
)

const (
	formatRaw     = "application/vnd.ipld.raw"
	formatCar     = "application/vnd.ipld.car"
	formatTar     = "application/x-tar"
	formatDagJSON = "application/vnd.ipld.dag-json"
	formatDagCbor = "application/vnd.ipld.dag-cbor"
	formatJSON    = "application/json"
	formatCbor    = "application/cbor"
)

// getHandler dispatches incoming requests to the appropriate handler based on the format requested in the Accept header or 'format' query parameter
func (hs *HttpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	tkPayload, err := hs.verifyToken(w, r)
	if err != nil {
		log.Warnf("verify token error:%s", err.Error())
		http.Error(w, fmt.Sprintf("verify token error : %s", err.Error()), http.StatusUnauthorized)
		return
	}

	if len(tkPayload.ID) > 0 {
		hs.tokens.Store(tkPayload.ID, struct{}{})
		defer func() {
			hs.tokens.Delete(tkPayload.ID)
		}()
	}

	respFormat, formatParams, err := customResponseFormat(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("processing the Accept header error: %s", err.Error()), http.StatusBadRequest)
		return
	}

	switch respFormat {
	case "", formatJSON, formatCbor: // The implicit response format is UnixFS
		hs.serveUnixFS(w, r, tkPayload)
	case formatRaw:
		hs.serveRawBlock(w, r, tkPayload)
	case formatCar:
		hs.serveCar(w, r, tkPayload, formatParams["version"])
	case formatTar:
		hs.serveTAR(w, r, tkPayload)
	case formatDagJSON, formatDagCbor:
		hs.serveCodec(w, r, tkPayload)
	default: // catch-all for unsuported application/vnd.*
		http.Error(w, fmt.Sprintf("unsupported format %s", respFormat), http.StatusBadRequest)
		return
	}
}

// verifyToken checks the request's token to make sure it was authorized
func (hs *HttpServer) verifyToken(w http.ResponseWriter, r *http.Request) (*types.TokenPayload, error) {
	if hs.schedulerPublicKey == nil {
		return nil, fmt.Errorf("scheduler public key not exist, can not verify sign")
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	// TODO check if come from browser
	if len(data) == 0 && len(r.UserAgent()) > 0 {
		rootCID, err := getCIDFromURLPath(r.URL.Path)
		if err != nil {
			return nil, err
		}
		return &types.TokenPayload{AssetCID: rootCID.String()}, nil
	}

	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)

	esc := &types.Token{}
	err = dec.Decode(esc)
	if err != nil {
		return nil, xerrors.Errorf("decode token error %w", err)
	}

	sign, err := hex.DecodeString(esc.Sign)
	if err != nil {
		return nil, err
	}

	cipherText, err := hex.DecodeString(esc.CipherText)
	if err != nil {
		return nil, err
	}

	rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	err = rsa.VerifySign(hs.schedulerPublicKey, sign, cipherText)
	if err != nil {
		return nil, err
	}

	mgs, err := rsa.Decrypt(cipherText, hs.privateKey)
	if err != nil {
		return nil, err
	}

	buffer = bytes.NewBuffer(mgs)
	dec = gob.NewDecoder(buffer)

	tkPayload := &types.TokenPayload{}
	err = dec.Decode(tkPayload)
	if err != nil {
		return nil, err
	}
	return tkPayload, nil
}

// customResponseFormat checks the request's Accept header and query parameters to determine the desired response format
func customResponseFormat(r *http.Request) (mediaType string, params map[string]string, err error) {
	if formatParam := r.URL.Query().Get("format"); formatParam != "" {
		// translate query param to a content type
		switch formatParam {
		case "raw":
			return formatRaw, nil, nil
		case "car":
			return formatCar, nil, nil
		case "tar":
			return formatTar, nil, nil
		case "json":
			return formatJSON, nil, nil
		case "cbor":
			return formatCbor, nil, nil
		case "dag-json":
			return formatDagJSON, nil, nil
		case "dag-cbor":
			return formatDagCbor, nil, nil
		}
	}
	// Browsers and other user agents will send Accept header with generic types like:
	// Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8
	// We only care about explciit, vendor-specific content-types.
	for _, accept := range r.Header.Values("Accept") {
		// respond to the very first ipld content type
		if strings.HasPrefix(accept, "application/vnd.ipld") ||
			strings.HasPrefix(accept, "application/x-tar") ||
			strings.HasPrefix(accept, "application/json") ||
			strings.HasPrefix(accept, "application/cbor") ||
			strings.HasPrefix(accept, "application/vnd.ipfs") {
			mediatype, params, err := mime.ParseMediaType(accept)
			if err != nil {
				return "", nil, err
			}
			return mediatype, params, nil
		}
	}
	return "", nil, nil
}
