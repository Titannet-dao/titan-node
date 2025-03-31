package httpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/lib/carutil"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
)

type taskStatus string

const (
	Pending    taskStatus = "pending"
	InProgress taskStatus = "inprogress"
	Completed  taskStatus = "complete"
	Failed     taskStatus = "failed"
)

func (hs *HttpServer) uploadv3StatusHandler(w http.ResponseWriter, r *http.Request) {

	setAccessControlAllowForHeader(w)
	if r.Method == http.MethodOptions {
		return
	}

	if r.Method != http.MethodGet {
		uploadResult(w, -1, fmt.Sprintf("only allow get method, http status code %d, your method %s", http.StatusMethodNotAllowed, r.Method))
		return
	}

	task := r.URL.Query().Get("task")
	if task == "" {
		uploadResult(w, -1, fmt.Sprintf("task empty, http status code %d", http.StatusBadRequest))
		return
	}

	v, ok := hs.v3Progress.Load(task)
	if !ok {
		uploadResult(w, -1, fmt.Sprintf("task not found, http status code %d", http.StatusNotFound))
		return
	}

	taskInfo, ok := v.(*downloadProgressTaskReaderCloser)
	if !ok {
		uploadResult(w, -1, fmt.Sprintf("get task progress error, http status code %d", http.StatusInternalServerError))
		return
	}

	type TaskInfo struct {
		TaskID      string
		FileName    string
		TotalSize   int64
		CurrentSize int64
		Progress    float64
		Cid         string
		Status      string
	}

	ret := TaskInfo{
		TaskID:      taskInfo.taskId,
		FileName:    taskInfo.filename,
		TotalSize:   taskInfo.totalSize,
		CurrentSize: taskInfo.currentSize,
		Progress:    taskInfo.progress,
		Status:      string(taskInfo.Status),
	}
	if taskInfo.cid.String() != "b" {
		ret.Cid = taskInfo.cid.String()
	}
	buf, err := json.Marshal(ret)

	if err != nil {
		log.Errorf("marshal error %s", err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusInternalServerError))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(buf); err != nil {
		log.Errorf("write error %s", err.Error())
	}
}

type uploadv3Req struct {
	Url   string `json:"url"`
	Async bool   `json:"async"`
}

// uploadv3 gives a url download mode, and return the cid async
func (hs *HttpServer) uploadv3Handler(w http.ResponseWriter, r *http.Request) {
	log.Debug("uploadv3Handler")
	setAccessControlAllowForHeader(w)
	if r.Method == http.MethodOptions {
		return
	}

	if r.Method != http.MethodPost {
		uploadResult(w, -1, fmt.Sprintf("only allow post method, http status code %d", http.StatusMethodNotAllowed))
		return
	}

	userID, _, err := verifyToken(r, hs.apiSecret)
	if err != nil {
		log.Errorf("verfiy token error: %s", err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusUnauthorized))
		return
	}

	var req uploadv3Req
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		log.Errorf("error decoding JSON: %s", err.Error())
		uploadResult(w, -1, fmt.Sprintf("invalid JSON format, http status code %d", http.StatusBadRequest))
		return
	}

	if req.Url == "" {
		uploadResult(w, -1, fmt.Sprintf("url invalid, http status code %d", http.StatusBadRequest))
		return
	}

	log.Debugf("user %s upload from %s", userID, req.Url)

	headResp, err := http.Head(req.Url)
	if err != nil {
		log.Errorf("http head to %s error: %s", req.Url, err.Error())
		uploadResult(w, -1, fmt.Sprintf("%s http status code %d", err.Error(), http.StatusBadRequest))
		return
	}
	defer headResp.Body.Close()

	// ignore 200/405
	if headResp.StatusCode != http.StatusOK && headResp.StatusCode != http.StatusMethodNotAllowed {
		uploadResult(w, -1, fmt.Sprintf("remote source file not available %s, http status code %d", req.Url, headResp.StatusCode))
		return
	}

	// limit max concurrent
	semaphore <- struct{}{}
	defer func() { <-semaphore }()

	taskid, rootCid, statusCode, err := hs.handleDownload(req.Url, int64(hs.maxSizeOfUploadFile), req.Async)
	if err != nil {
		uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), statusCode))
		return
	}

	type Result struct {
		Code int    `json:"code"`
		Err  int    `json:"err"`
		Msg  string `json:"msg"`
		Cid  string `json:"cid"`
		Task string `json:"task"`
	}

	ret := Result{Code: 0, Err: 0, Msg: "", Task: taskid}
	if rootCid.String() != "b" {
		ret.Cid = rootCid.String()
	}

	buf, err := json.Marshal(ret)
	if err != nil {
		log.Errorf("marshal error %s", err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if _, err = w.Write(buf); err != nil {
		log.Errorf("write error %s", err.Error())
	}
}

type downloadProgressTaskReaderCloser struct {
	filename    string
	taskId      string
	reader      io.ReadCloser
	totalSize   int64
	currentSize int64
	onProgress  func(int64, int64)
	progress    float64
	cid         cid.Cid
	Status      taskStatus
}

func (dp *downloadProgressTaskReaderCloser) Read(p []byte) (int, error) {
	n, err := dp.reader.Read(p)
	dp.currentSize += int64(n)
	dp.progress = float64(dp.currentSize) / float64(dp.totalSize) * 100
	if dp.onProgress != nil {
		dp.onProgress(dp.currentSize, dp.totalSize)
	}
	return n, err
}

func (dp *downloadProgressTaskReaderCloser) Close() error {
	return dp.reader.Close()
}

func (hs *HttpServer) handleDownload(addr string, maxBytes int64, async bool) (string, cid.Cid, int, error) {
	// Get the uploaded with addr
	parsedUrl, err := url.ParseRequestURI(addr)
	if err != nil {
		log.Errorf("invalid source url: %s", addr)
		return "", cid.Cid{}, http.StatusBadRequest, fmt.Errorf("invalid url %s, http status code %d", addr, http.StatusBadRequest)
	}

	resp, err := http.Get(addr)
	if err != nil {
		log.Errorf("http.Get %s error: %s", addr, err.Error())
		return "", cid.Cid{}, http.StatusInternalServerError, err
	}
	// defer resp.Body.Close()

	fileName := getFileName(resp, parsedUrl)
	log.Debugf("download %s file name: %s", addr, fileName)

	if resp.StatusCode != http.StatusOK {
		log.Errorf("failed to download file from %s, status code: %d", addr, resp.StatusCode)
		return "", cid.Cid{}, resp.StatusCode, fmt.Errorf("failed to download file: status code %d", resp.StatusCode)
	}

	limitReader := http.MaxBytesReader(nil, resp.Body, maxBytes)

	taskID := uuid.NewString()
	progressReader := &downloadProgressTaskReaderCloser{
		taskId:    taskID,
		filename:  fileName,
		reader:    limitReader,
		totalSize: resp.ContentLength,
		onProgress: func(cur, total int64) {
			v := &downloadProgressTaskReaderCloser{
				taskId:      taskID,
				filename:    fileName,
				currentSize: cur,
				totalSize:   total,
				progress:    float64(cur) / float64(total) * 100,
				Status:      InProgress,
			}
			hs.v3Progress.Store(taskID, v)
		},
	}

	if async {
		go func() {
			_, err = hs.downloadTask(fileName, progressReader)
			if err != nil {
				log.Errorf("download failed %s", err.Error())
			}
		}()
		return taskID, cid.Cid{}, http.StatusAccepted, nil
	}

	rootCID, err := hs.downloadTask(fileName, progressReader)
	if err != nil {
		log.Errorf("download failed %s", err.Error())
		return "", cid.Cid{}, http.StatusInternalServerError, err
	}
	return "", rootCID, http.StatusOK, err
}

func (hs *HttpServer) downloadTask(fileName string, reader *downloadProgressTaskReaderCloser) (cid.Cid, error) {
	defer reader.Close()

	hs.setStatus(reader, Pending)
	assetDir, err := hs.asset.AllocatePathWithSize(reader.totalSize)
	if err != nil {
		hs.setStatus(reader, Failed)
		return cid.Cid{}, fmt.Errorf("cannot allocate storage for file: %s", err.Error())
	}

	assetTempDirPath := path.Join(assetDir, reader.taskId)
	if err = os.Mkdir(assetTempDirPath, 0755); err != nil {
		hs.setStatus(reader, Failed)
		return cid.Cid{}, fmt.Errorf("mkdir failed: %s", err.Error())
	}
	defer os.RemoveAll(assetTempDirPath)

	assetPath := path.Join(assetTempDirPath, fileName)
	out, err := os.Create(assetPath)
	if err != nil {
		hs.setStatus(reader, Failed)
		return cid.Cid{}, fmt.Errorf("create file failed: %s, path: %s", err.Error(), assetPath)
	}
	defer out.Close()

	if _, err := io.Copy(out, reader); err != nil {
		hs.setStatus(reader, Failed)
		return cid.Cid{}, fmt.Errorf("save file failed: %s, path: %s", err.Error(), assetPath)
	}

	tempCarFile := path.Join(assetDir, uuid.NewString())
	rootCID, err := carutil.CreateCar(assetPath, tempCarFile)
	if err != nil {
		hs.setStatus(reader, Failed)
		return cid.Cid{}, fmt.Errorf("create car failed: %s, path: %s", err.Error(), tempCarFile)
	}
	defer os.RemoveAll(tempCarFile)

	if isExists, err := hs.asset.AssetExists(rootCID); err != nil {
		hs.setStatus(reader, Failed)
		return cid.Cid{}, err
	} else if isExists {
		reader.cid = rootCID
		hs.setStatus(reader, Completed)
		log.Debugf("asset %s already exists", rootCID.String())
		return rootCID, nil
	}

	if err = hs.saveCarFile(context.Background(), tempCarFile, rootCID); err != nil {
		hs.setStatus(reader, Failed)
		return cid.Cid{}, fmt.Errorf("save car file failed: %s, file: %s", err.Error(), fileName)
	}

	reader.cid = rootCID
	hs.setStatus(reader, Completed)

	// delete taskinfo after 24 hours
	time.AfterFunc(24*time.Hour, func() {
		log.Infof("remove upload task %s after 24 hours, file: %s", reader.taskId, fileName)
		hs.v3Progress.Delete(reader.taskId)
	})

	return rootCID, nil
}

func (hs *HttpServer) setStatus(reader *downloadProgressTaskReaderCloser, status taskStatus) {
	reader.Status = status
	hs.v3Progress.Store(reader.taskId, reader)
}

func getFileName(resp *http.Response, url *url.URL) string {
	fileName := getFileNameFromResponse(resp)
	if fileName == "" {
		fileName = getFileNameFromUrl(url)
	}
	return fileName
}

func getFileNameFromResponse(resp *http.Response) string {
	_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Disposition"))
	if err != nil {
		return ""
	}
	return params["filename"]
}

func getFileNameFromUrl(url *url.URL) string {
	pathSegments := strings.Split(url.Path, "/")
	fileName := pathSegments[len(pathSegments)-1]
	return fileName
}
