package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/lib/carutil"
	fscrypto "github.com/Filecoin-Titan/titan/node/httpserver/crypto"

	"github.com/gbrlsnchs/jwt/v3"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

func (hs *HttpServer) uploadv2Handler(w http.ResponseWriter, r *http.Request) {
	log.Debug("uploadv2Handler")
	setAccessControlAllowForHeader(w)
	if r.Method == http.MethodOptions {
		return
	}

	if r.Method != http.MethodPost {
		uploadResult(w, -1, fmt.Sprintf("only allow post method, http status code %d", http.StatusMethodNotAllowed))
		return
	}

	userID, pass, err := verifyToken(r, hs.apiSecret)
	if err != nil {
		log.Errorf("verfiy token error: %s", err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusUnauthorized))
		return
	}

	log.Infof("user %s upload file", userID)

	// limit max concurrent
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	// limit size
	r.Body = http.MaxBytesReader(w, r.Body, int64(hs.maxSizeOfUploadFile))

	var statusCode int
	var root cid.Cid
	contentType := getContentType(r)
	switch contentType {
	case "multipart/form-data":
		root, statusCode, err = hs.handleUploadFileV2(r, pass)
	default:
		log.Errorf("unsupported Content-type %s", contentType)
		statusCode = http.StatusBadRequest
		err = fmt.Errorf("unsupported Content-type %s", contentType)
	}

	if err != nil {
		log.Debugw("upload file error", "error", err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), statusCode))
		return
	}

	type Result struct {
		Code int    `json:"code"`
		Err  int    `json:"err"`
		Msg  string `json:"msg"`
		Cid  string `json:"cid"`
	}

	ret := Result{Code: 0, Err: 0, Msg: "", Cid: root.String()}
	buf, err := json.Marshal(ret)
	if err != nil {
		log.Errorf("marshal error %s", err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(buf); err != nil {
		log.Errorf("write error %s", err.Error())
	}

}

func verifyToken(r *http.Request, apiSecret *jwt.HMACSHA) (string, string, error) {
	token := r.Header.Get("Authorization")
	if token == "" {
		token = r.FormValue("token")
		if token != "" {
			token = "Bearer " + token
		}
	}

	if token != "" {
		if !strings.HasPrefix(token, "Bearer ") {
			return "", "", fmt.Errorf("missing Bearer prefix in auth header")
		}
		token = strings.TrimPrefix(token, "Bearer ")
	}

	payload := &types.JWTPayload{}
	if _, err := jwt.Verify([]byte(token), apiSecret, payload); err != nil {
		return "", "", err
	}

	return payload.ID, payload.FilePassNonce, nil
}

func (hs *HttpServer) handleUploadFileV2(r *http.Request, passNonce string) (cid.Cid, int, error) {
	// Get the uploaded file
	file, header, err := r.FormFile("file")
	if err != nil {
		return cid.Cid{}, http.StatusBadRequest, err
	}
	defer file.Close()

	log.Debugw("handle upload file", "filename", header.Filename)
	fr := io.Reader(file)

	assetDir, err := hs.asset.AllocatePathWithSize(header.Size)
	if err != nil {
		log.Debugw("Allocate storage error", "error", err.Error())
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("can not allocate storage for file %s", err.Error())
	}

	assetTempDirPath := path.Join(assetDir, uuid.NewString())
	if err = os.Mkdir(assetTempDirPath, 0755); err != nil {
		log.Debugw("mkdir error", "error", err.Error())
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("mkdir fialed %s", err.Error())
	}
	defer os.RemoveAll(assetTempDirPath)

	if passNonce != "" {
		enf, n, err := fscrypto.Encrypt(file, []byte(passNonce), assetTempDirPath)
		if err != nil {
			return cid.Cid{}, http.StatusInternalServerError, err
		}
		log.Debugf("encrypt file %s, size %d", header.Filename, n)
		fr = enf
	}

	assetPath := path.Join(assetTempDirPath, header.Filename)
	out, err := os.Create(assetPath)
	if err != nil {
		log.Debugw("create file error", "error", err.Error())
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("create file failed: %s, path: %s", err.Error(), assetPath)
	}
	defer out.Close()

	if _, err := io.Copy(out, fr); err != nil {
		log.Debugw("copy file error", "error", err.Error())
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("save file failed: %s, path: %s", err.Error(), assetPath)
	}

	tempCarFile := path.Join(assetDir, uuid.NewString())
	rootCID, err := carutil.CreateCar(assetPath, tempCarFile)
	if err != nil {
		log.Debugw("create car error", "error", err.Error())
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("create car failed: %s, path: %s", err.Error(), tempCarFile)
	}
	defer os.RemoveAll(tempCarFile)

	var isExists bool
	if isExists, err = hs.asset.AssetExists(rootCID); err != nil {
		log.Debugw("check asset exist error", "error", err.Error())
		return cid.Cid{}, http.StatusInternalServerError, err
	} else if isExists {
		log.Debugf("asset %s already exist", rootCID.String())
		// return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("asset %s already exist, file %s", rootCID.String(), header.Filename)
		return rootCID, http.StatusOK, nil
	}

	if err = hs.saveCarFile(context.Background(), tempCarFile, rootCID); err != nil {
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("save car file failed %s, file %s", err.Error(), header.Filename)
	}

	return rootCID, http.StatusOK, nil
}

func (hs *HttpServer) saveCarFile(ctx context.Context, tempCarFile string, root cid.Cid) error {
	f, err := os.Open(tempCarFile)
	if err != nil {
		log.Debugw("open car file error", "error", err.Error())
		return err
	}
	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		log.DPanicw("get car file size error", "error", err.Error())
		return err
	}
	log.Debugf("car file size %d", fInfo.Size())
	if err := hs.asset.SaveUserAsset(ctx, uuid.NewString(), root, fInfo.Size(), f); err != nil {
		log.Debugw("save asset error", "error", err.Error())
		return err
	}
	return nil
}
