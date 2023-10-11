package httpserver

import (
	"bytes"
	"context"
	"crypto"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
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

func (hs *HttpServer) isNeedRedirect(r *http.Request) bool {
	return r.Header.Get("Jwtauthorization") == "" && len(r.UserAgent()) > 0 && len(hs.webRedirect) > 0
}

// getHandler dispatches incoming requests to the appropriate handler based on the format requested in the Accept header or 'format' query parameter
func (hs *HttpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	tkPayload, err := hs.verifyToken(w, r)
	if err != nil {
		log.Warnf("verify token error: %s, url: %s", err.Error(), r.URL.String())

		errWeb, ok := err.(*api.ErrWeb)
		// check if titan storage web
		if ok && hs.isNeedRedirect(r) {
			url := fmt.Sprintf("%s?errCode=%d", hs.webRedirect, errWeb.Code)
			http.Redirect(w, r, url, http.StatusMovedPermanently)
			return
		}

		errCode := -1
		if errWeb != nil {
			errCode = errWeb.Code
		}

		authResult(w, errCode, fmt.Errorf("verify token error: %s", err.Error()))
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

	assetCID := tkPayload.AssetCID
	speedCountWriter := &SpeedCountWriter{w: w, startTime: time.Now()}
	var statusCode int
	var isDirectory bool

	switch respFormat {
	case "", formatJSON, formatCbor: // The implicit response format is UnixFS
		if isDirectory, statusCode, err = hs.serveUnixFS(speedCountWriter, r, assetCID); isDirectory {
			log.Debugf("serveUnixDir size %d, speed %dB/s, cost time %fms", speedCountWriter.dataSize, speedCountWriter.speed(), speedCountWriter.CostTime())
			return
		}
	case formatRaw:
		statusCode, err = hs.serveRawBlock(speedCountWriter, r, assetCID)
	case formatCar:
		statusCode, err = hs.serveCar(speedCountWriter, r, assetCID, formatParams["version"])
	case formatTar:
		statusCode, err = hs.serveTAR(speedCountWriter, r, assetCID)
	case formatDagJSON, formatDagCbor:
		statusCode, err = hs.serveCodec(speedCountWriter, r, assetCID)
	default: // catch-all for unsuported application/vnd.*
		statusCode = http.StatusBadRequest
		err = fmt.Errorf("unsupported format %s", respFormat)
	}

	if err != nil {
		http.Error(w, err.Error(), statusCode)
		log.Errorf("get handler error %s", err.Error())
		return
	}

	log.Debugf("tokenID:%s, clientID:%s, download size %d, speed %d, cost time %fms", tkPayload.ID, tkPayload.ClientID, speedCountWriter.dataSize, speedCountWriter.speed(), speedCountWriter.CostTime())
	// stat upload speed
	report := speedCountWriter.generateReport(tkPayload)
	if report != nil {
		hs.reporter.addReport(report)
	}

	if len(tkPayload.ID) == 0 && len(tkPayload.ClientID) != 0 && speedCountWriter.speed() > 0 {
		hs.scheduler.UserAssetDownloadResult(context.Background(), tkPayload.ClientID, tkPayload.AssetCID, speedCountWriter.dataSize, speedCountWriter.speed())
	}
}

// verifyToken checks the request's token to make sure it was authorized
func (hs *HttpServer) verifyToken(w http.ResponseWriter, r *http.Request) (*types.TokenPayload, error) {
	if hs.schedulerPublicKey == nil {
		return nil, fmt.Errorf("scheduler public key not exist, can not verify sign")
	}

	if token := r.Header.Get("User-Token"); len(token) > 0 {
		_, err := hs.scheduler.AuthVerify(context.TODO(), token)
		if err != nil {
			return nil, err
		}

		rootCID, err := getCIDFromURLPath(r.URL.Path)
		if err != nil {
			return nil, err
		}
		return &types.TokenPayload{AssetCID: rootCID.String()}, nil
	}

	if token := r.URL.Query().Get("token"); len(token) > 0 {
		return hs.parseJWTToken(token, r)
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

func (hs *HttpServer) parseJWTToken(token string, r *http.Request) (*types.TokenPayload, error) {
	jwtPayload, err := hs.scheduler.VerifyTokenWithLimitCount(context.Background(), token)
	if err != nil {
		return nil, err
	}

	payload := &types.AuthUserUploadDownloadAsset{}
	if err = json.Unmarshal([]byte(jwtPayload.Extend), payload); err != nil {
		return nil, err
	}

	root, err := getCIDFromURLPath(r.URL.Path)
	if err != nil {
		return nil, err
	}

	if root.String() != payload.AssetCID {
		return nil, fmt.Errorf("request asset cid %s, parse token cid %s", root.String(), payload.AssetCID)
	}

	return &types.TokenPayload{AssetCID: payload.AssetCID, ClientID: payload.UserID, Expiration: payload.Expiration}, nil
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
		if strings.HasPrefix(accept, "application/json") {
			return formatJSON, nil, nil
		}

		// respond to the very first ipld content type
		if strings.HasPrefix(accept, "application/vnd.ipld") ||
			strings.HasPrefix(accept, "application/x-tar") ||
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

func authResult(w http.ResponseWriter, code int, err error) {
	type AuthResult struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}

	ret := AuthResult{Code: code, Msg: err.Error()}
	buf, err := json.Marshal(ret)
	if err != nil {
		log.Errorf("marshal error %s", err.Error())
		return
	}

	w.Write(buf)
}
