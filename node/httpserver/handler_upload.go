package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/ipfs/go-cid"
)

const (
	maxUploadSize = 100 * 1024 * 1024 // 100 MB
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
		http.Error(w, "only allow post method", http.StatusMethodNotAllowed)
		return
	}

	payload, err := hs.verifyUserToken(r)
	if err != nil {
		log.Errorf("verfiy token error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	if payload.AssetSize > maxUploadSize {
		log.Errorf("max file size is: %d, current file size:%d", maxUploadSize, payload.AssetSize)
		http.Error(w, fmt.Sprintf("asset Size is %d, max size is %d", payload.AssetSize, maxUploadSize), http.StatusBadRequest)
		return
	}

	c, err := cid.Decode(payload.AssetCID)
	if err != nil {
		log.Errorf("decode asset cid error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = hs.asset.GetUploadingAsset(context.Background(), c)
	if err != nil {
		log.Errorf("get uploading asset error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// limit max concurrent
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	// limit size
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)

	contentType := getContentType(r)
	switch contentType {
	case "multipart/form-data":
		hs.handleUploadFormData(w, r, payload)
	case "application/car":
		hs.handleUploadCar(w, r, payload)
	default:
		log.Errorf("unsupported Content-type %s", contentType)
		http.Error(w, fmt.Sprintf("unsupported Content-type %s", contentType), http.StatusBadRequest)
	}
}

func getContentType(r *http.Request) string {
	contentType := r.Header.Get("Content-type")
	mediaType := strings.Split(contentType, ";")[0]
	return strings.TrimSpace(mediaType)
}

func (hs *HttpServer) verifyUserToken(r *http.Request) (*types.AuthUserUploadAsset, error) {
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

	payload := &types.AuthUserUploadAsset{}
	if _, err := jwt.Verify([]byte(token), hs.apiSecret, payload); err != nil {
		return nil, err
	}

	if time.Now().After(payload.Expiration) {
		return nil, fmt.Errorf("token is expire, userID %s, cid:%s", payload.UserID, payload.AssetCID)
	}

	return payload, nil
}

func (hs *HttpServer) handleUploadFormData(w http.ResponseWriter, r *http.Request, payload *types.AuthUserUploadAsset) {
	// try to read from multipart form
	err := r.ParseMultipartForm(maxUploadSize)
	if err != nil {
		http.Error(w, fmt.Sprintf("parse multipart form error %s", err.Error()), http.StatusBadRequest)
		return
	}

	root, err := cid.Decode(payload.AssetCID)
	if err != nil {
		log.Errorf("decode asset cid error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Get the uploaded file
	file, _, err := r.FormFile("file")
	if err != nil {
		// Handle error
		log.Errorf("read file from form error : %s", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	progressReader := newProgressReader(file, hs, root, payload.AssetSize)
	if err := hs.asset.SaveUserAsset(context.Background(), payload.UserID, root, payload.AssetSize, progressReader); err != nil {
		http.Error(w, fmt.Sprintf("upload file error %s", err.Error()), http.StatusInternalServerError)
		return
	}

	progress := &types.UploadProgress{TotalSize: payload.AssetSize, DoneSize: payload.AssetSize}
	if err := hs.asset.SetAssetUploadProgress(context.Background(), root, progress); err != nil {
		http.Error(w, fmt.Sprintf("set asset upload progress error %s", err.Error()), http.StatusInternalServerError)
		return
	}
}

func (hs *HttpServer) handleUploadCar(w http.ResponseWriter, r *http.Request, payload *types.AuthUserUploadAsset) {
	root, err := cid.Decode(payload.AssetCID)
	if err != nil {
		log.Errorf("decode asset cid error: %s", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	progressReader := newProgressReader(r.Body, hs, root, payload.AssetSize)
	if err := hs.asset.SaveUserAsset(context.Background(), payload.UserID, root, payload.AssetSize, progressReader); err != nil {
		http.Error(w, fmt.Sprintf("upload file error %s", err.Error()), http.StatusInternalServerError)
		return
	}

	progress := &types.UploadProgress{TotalSize: payload.AssetSize, DoneSize: payload.AssetSize}
	if err := hs.asset.SetAssetUploadProgress(context.Background(), root, progress); err != nil {
		log.Debugf("SetAssetUploadProgress error %s", err.Error())
	}
}

func setAccessControlAllowForHeader(w http.ResponseWriter) {
	allowedHeaders := "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, Unauthorized, X-CSRF-Token"

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
	w.Header().Set("Access-Control-Expose-Headers", "Authorization")
}
