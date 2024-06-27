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

	userID, err := verifyToken(r, hs.apiSecret)
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
		root, statusCode, err = hs.handleUploadFileV2(r)
	default:
		log.Errorf("unsupported Content-type %s", contentType)
		statusCode = http.StatusBadRequest
		err = fmt.Errorf("unsupported Content-type %s", contentType)
	}

	if err != nil {
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

func verifyToken(r *http.Request, apiSecret *jwt.HMACSHA) (string, error) {
	token := r.Header.Get("Authorization")
	if token == "" {
		token = r.FormValue("token")
		if token != "" {
			token = "Bearer " + token
		}
	}

	if token != "" {
		if !strings.HasPrefix(token, "Bearer ") {
			return "", fmt.Errorf("missing Bearer prefix in auth header")
		}
		token = strings.TrimPrefix(token, "Bearer ")
	}

	payload := &types.JWTPayload{}
	if _, err := jwt.Verify([]byte(token), apiSecret, payload); err != nil {
		return "", err
	}

	return payload.ID, nil
}

func (hs *HttpServer) handleUploadFileV2(r *http.Request) (cid.Cid, int, error) {
	// Get the uploaded file
	file, header, err := r.FormFile("file")
	if err != nil {
		return cid.Cid{}, http.StatusBadRequest, err
	}
	defer file.Close()

	assetDir, err := hs.asset.AllocatePathWithSize(header.Size)
	if err != nil {
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("can not allocate storage for file %s", err.Error())
	}

	assetTempDirPath := path.Join(assetDir, uuid.NewString())
	if err = os.Mkdir(assetTempDirPath, 0755); err != nil {
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("mkdir fialed %s", err.Error())
	}
	defer os.RemoveAll(assetTempDirPath)

	assetPath := path.Join(assetTempDirPath, header.Filename)
	out, err := os.Create(assetPath)
	if err != nil {
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("create file failed: %s, path: %s", err.Error(), assetPath)
	}
	defer out.Close()

	if _, err := io.Copy(out, file); err != nil {
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("save file failed: %s, path: %s", err.Error(), assetPath)
	}

	tempCarFile := path.Join(assetDir, uuid.NewString())
	rootCID, err := carutil.CreateCar(assetPath, tempCarFile)
	if err != nil {
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("create car failed: %s, path: %s", err.Error(), tempCarFile)
	}
	defer os.RemoveAll(tempCarFile)

	var isExists bool
	if isExists, err = hs.asset.AssetExists(rootCID); err != nil {
		return cid.Cid{}, http.StatusInternalServerError, err
	} else if isExists {
		log.Debugf("asset %s already exist", rootCID.String())
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("asset %s already exist, file %s", header.Filename)
	}

	if err = hs.saveCarFile(context.Background(), tempCarFile, rootCID); err != nil {
		return cid.Cid{}, http.StatusInternalServerError, fmt.Errorf("save car file failed %s, file %s", err.Error(), header.Filename)
	}

	return rootCID, http.StatusOK, nil
}

func (hs *HttpServer) saveCarFile(ctx context.Context, tempCarFile string, root cid.Cid) error {
	f, err := os.Open(tempCarFile)
	if err != nil {
		return err
	}
	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		return err
	}
	log.Debugf("car file size %d", fInfo.Size())
	if err := hs.asset.SaveUserAsset(ctx, uuid.NewString(), root, fInfo.Size(), f); err != nil {
		return err
	}
	return nil
}
