package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

const (
	// maxUploadSize = 104857600 // 100 MB
	maxConcurrent = 5
)

var semaphore = make(chan struct{}, maxConcurrent)

func (hs *HttpServer) uploadHandler(w http.ResponseWriter, r *http.Request) {
	log.Debug("uploadHandler")
	setAccessControlAllowForHeader(w)
	if r.Method == http.MethodOptions {
		return
	}

	if r.Method != http.MethodPost {
		uploadResult(w, -1, fmt.Sprintf("only allow post method, http status code %d", http.StatusMethodNotAllowed))
		return
	}

	payload, err := hs.verifyUserToken(r)
	if err != nil {
		log.Errorf("verfiy token error: %s", err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusUnauthorized))
		return
	}

	if payload.AssetSize > int64(hs.maxSizeOfUploadFile) {
		log.Errorf("max file size is: %d, current file size:%d", hs.maxSizeOfUploadFile, payload.AssetSize)
		uploadResult(w, -1, fmt.Sprintf("asset Size %d, out of max size %d, http status code %d", payload.AssetSize, hs.maxSizeOfUploadFile, http.StatusBadRequest))
		return
	}

	c, err := cid.Decode(payload.AssetCID)
	if err != nil {
		log.Errorf("decode asset cid error: %s", err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusInternalServerError))
		return
	}

	_, err = hs.asset.GetUploadingAsset(context.Background(), c)
	if err != nil {
		log.Errorf("get uploading asset error: %s", err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusBadRequest))
		return
	}

	// limit max concurrent
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	// limit size
	r.Body = http.MaxBytesReader(w, r.Body, int64(hs.maxSizeOfUploadFile))

	var statusCode int
	contentType := getContentType(r)
	switch contentType {
	case "multipart/form-data":
		statusCode, err = hs.handleUploadFormData(r, payload)
	case "application/car":
		statusCode, err = hs.handleUploadCar(r, payload)
	default:
		log.Errorf("unsupported Content-type %s", contentType)
		statusCode = http.StatusBadRequest
		err = fmt.Errorf("unsupported Content-type %s", contentType)
	}

	code := 0
	errMsg := "Upload succeeded"
	if statusCode != http.StatusOK {
		code = -1
		errMsg = fmt.Sprintf("%s, http status code %d", err.Error(), statusCode)
	}

	if err = uploadResult(w, code, errMsg); err != nil {
		log.Errorf("uploadResult %s", err.Error())
	}
}

func getContentType(r *http.Request) string {
	contentType := r.Header.Get("Content-type")
	mediaType := strings.Split(contentType, ";")[0]
	return strings.TrimSpace(mediaType)
}

func (hs *HttpServer) verifyUserToken(r *http.Request) (*types.AuthUserUploadDownloadAsset, error) {
	token := r.Header.Get("Authorization")
	if token == "" {
		token = r.FormValue("token")
		if token != "" {
			token = "Bearer " + token
		}
	}

	if token != "" {
		if !strings.HasPrefix(token, "Bearer ") {
			return nil, fmt.Errorf("missing Bearer prefix in auth header")
		}
		token = strings.TrimPrefix(token, "Bearer ")
	}

	payload := &types.AuthUserUploadDownloadAsset{}
	if _, err := jwt.Verify([]byte(token), hs.apiSecret, payload); err != nil {
		return nil, err
	}

	if time.Now().After(payload.Expiration) {
		return nil, fmt.Errorf("token is expire, userID %s, cid:%s", payload.UserID, payload.AssetCID)
	}

	return payload, nil
}

func (hs *HttpServer) handleUploadFormData(r *http.Request, payload *types.AuthUserUploadDownloadAsset) (int, error) {
	root, err := cid.Decode(payload.AssetCID)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	// Get the uploaded file
	file, _, err := r.FormFile("file")
	if err != nil {
		return http.StatusBadRequest, err
	}
	defer file.Close()

	log.Debugf("handleUploadFormData asset cid %s, size:%d", payload.AssetCID, payload.AssetSize)

	progressReader := newProgressReader(file, hs, root, payload.AssetSize)
	if err := hs.asset.SaveUserAsset(context.Background(), payload.UserID, root, payload.AssetSize, progressReader); err != nil {
		return http.StatusInternalServerError, xerrors.Errorf("save user asset error %w", err.Error())
	}

	progress := &types.UploadProgress{TotalSize: payload.AssetSize, DoneSize: payload.AssetSize}
	if err := hs.asset.SetAssetUploadProgress(context.Background(), root, progress); err != nil {
		return http.StatusInternalServerError, xerrors.Errorf("set asset upload progress error %w", err.Error())
	}

	return http.StatusOK, nil
}

func (hs *HttpServer) handleUploadCar(r *http.Request, payload *types.AuthUserUploadDownloadAsset) (int, error) {
	root, err := cid.Decode(payload.AssetCID)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	progressReader := newProgressReader(r.Body, hs, root, payload.AssetSize)
	if err := hs.asset.SaveUserAsset(context.Background(), payload.UserID, root, payload.AssetSize, progressReader); err != nil {
		return http.StatusInternalServerError, xerrors.Errorf("save user asset error %w", err.Error())
	}

	progress := &types.UploadProgress{TotalSize: payload.AssetSize, DoneSize: payload.AssetSize}
	if err := hs.asset.SetAssetUploadProgress(context.Background(), root, progress); err != nil {
		return http.StatusInternalServerError, xerrors.Errorf("set asset upload progress error %w", err.Error())

	}

	return http.StatusOK, nil
}

func uploadResult(w http.ResponseWriter, code int, msg string) error {
	type Result struct {
		Code int    `json:"code"`
		Err  int    `json:"err"`
		Msg  string `json:"msg"`
	}

	ret := Result{Code: code, Err: 0, Msg: msg}
	buf, err := json.Marshal(ret)
	if err != nil {
		return err
	}

	w.WriteHeader(http.StatusOK)
	_, err = w.Write(buf)
	return err
}
