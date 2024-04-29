package clib

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/google/uuid"
)

const (
	maxNumOfTask                  = 5
	downloadTaskStatusFailed      = "failed"
	downloadTaskStatusSuccessed   = "successed"
	downloadTaskStatusDownloading = "downloading"
)

type Progress struct {
	TotalSize int64
	DoneSize  int64
}

type Downloader struct {
	httpClient *http.Client
	taskList   []*downloadingTask
	lock       *sync.Mutex
}

func newDownloader() *Downloader {
	return &Downloader{
		httpClient: client.NewHTTP3Client(),
		taskList:   make([]*downloadingTask, 0),
		lock:       &sync.Mutex{},
	}
}

func (d *Downloader) downloadFile(req *DownloadFileReq) error {
	task := &downloadingTask{id: uuid.NewString(), req: req, progress: &Progress{}, httpClient: d.httpClient}
	if err := d.addTask(task); err != nil {
		return err
	}

	go func() {
		ctx, cancle := context.WithCancel(context.Background())
		defer cancle()

		task.cancelFunc = cancle

		if err := task.doDownload(ctx); err != nil {
			log.Errorf("doDownload error %s", err.Error())
		} else {
			log.Infof("download %s %s complete", task.req.CID, task.req.DownloadPath)
		}

		d.removeTask(task)

	}()

	return nil

}

func (d *Downloader) addTask(task *downloadingTask) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	if len(d.taskList) >= maxNumOfTask {
		return fmt.Errorf("The number of download task is out of %d", len(d.taskList))
	}

	if len(task.req.CID) == 0 {
		return fmt.Errorf("CID can not emtpy")
	}

	if len(task.req.DownloadPath) == 0 {
		return fmt.Errorf("download_path can not empty")
	}

	if len(task.req.LocatorURL) == 0 {
		return fmt.Errorf("LocatorURL can not empty")
	}

	if d.downloadTaskIsExist(task.req) {
		return fmt.Errorf("File %s or %s is downloading", task.req.CID, task.req.DownloadPath)
	}

	if ok, err := d.fileIsExist(task.req.DownloadPath); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("File %s is exist", task.req.DownloadPath)
	}

	dir := filepath.Dir(task.req.DownloadPath)
	if ok, err := d.fileIsExist(dir); err != nil {
		return err
	} else if !ok {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	d.taskList = append(d.taskList, task)
	return nil
}

func (d *Downloader) removeTask(task *downloadingTask) {
	d.lock.Lock()
	defer d.lock.Unlock()

	for i, t := range d.taskList {
		if t.id == task.id {
			d.taskList = append(d.taskList[:i], d.taskList[i+1:]...)
			return
		}
	}

}

func (d *Downloader) downloadTaskIsExist(req *DownloadFileReq) bool {
	for _, task := range d.taskList {
		if task.req.CID == req.CID || task.req.DownloadPath == req.DownloadPath {
			return true
		}
	}

	return false
}

func (d *Downloader) fileIsExist(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err

}

func (d *Downloader) cancelDownloadTask(filePath string) error {
	for _, task := range d.taskList {
		if task.req.DownloadPath == filePath {
			task.cancelFunc()
			return nil
		}
	}

	return nil
}

func (d *Downloader) queryProgress(filePath string) (*DownloadProgressResult, error) {
	for _, task := range d.taskList {
		if task.req.DownloadPath == filePath {
			return taskToDownloadProgress(task), nil
		}
	}

	info, err := os.Stat(filePath)
	if err == nil {
		return fileInfoToDownloadPregress(info, filePath), nil
	}

	return donwloadFailedResult(filePath), nil
}

func taskToDownloadProgress(task *downloadingTask) *DownloadProgressResult {
	result := &DownloadProgressResult{FilePath: task.req.DownloadPath}
	result.Status = downloadTaskStatusDownloading
	result.TotalSize = task.progress.TotalSize
	result.DoneSize = task.progress.DoneSize
	return result
}

func fileInfoToDownloadPregress(info fs.FileInfo, filePath string) *DownloadProgressResult {
	result := &DownloadProgressResult{FilePath: filePath}
	result.Status = downloadTaskStatusSuccessed
	result.TotalSize = info.Size()
	result.DoneSize = info.Size()
	return result
}

func donwloadFailedResult(filePath string) *DownloadProgressResult {
	result := &DownloadProgressResult{FilePath: filePath}
	result.Status = downloadTaskStatusFailed
	result.TotalSize = 0
	result.DoneSize = 0
	return result
}

type downloadingTask struct {
	id         string
	req        *DownloadFileReq
	progress   *Progress
	httpClient *http.Client
	cancelFunc context.CancelFunc
}

func (dt *downloadingTask) getDownloadInfos(ctx context.Context) ([]*types.AssetSourceDownloadInfoRsp, error) {
	locator, closer, err := client.NewLocator(ctx, dt.req.LocatorURL, nil, jsonrpc.WithHTTPClient(dt.httpClient))
	if err != nil {
		fmt.Printf("new locator error %s\n", err.Error())
		return nil, err
	}
	defer closer()

	downloadInfos, err := locator.GetAssetSourceDownloadInfos(ctx, dt.req.CID)
	if err != nil {
		fmt.Printf("edge download infos error: %s\n", err.Error())
		return nil, err
	}

	infos := make([]*types.AssetSourceDownloadInfoRsp, 0, len(downloadInfos))
	for _, downloadInfo := range downloadInfos {
		if len(downloadInfo.SourceList) > 0 {
			infos = append(infos, downloadInfo)
		}
	}
	return infos, nil

}

func (dt *downloadingTask) doDownload(ctx context.Context) error {
	downloadInfos, err := dt.getDownloadInfos(ctx)
	if err != nil {
		return fmt.Errorf("get download info error:%s", err.Error())
	}

	if len(downloadInfos) == 0 {
		return fmt.Errorf("can not get node for asset %s", dt.req.CID)
	}

	for _, downloadInfo := range downloadInfos {
		for _, source := range downloadInfo.SourceList {
			startTime := time.Now()

			if err := dt.doDownloadFile(ctx, source); err != nil {
				log.Warnf("download file from %s error %s", source.NodeID, err.Error())
				continue
			}

			// submit workload
			costTime := time.Since(startTime) / time.Millisecond
			workload := types.Workload{SourceID: source.NodeID, DownloadSize: dt.progress.TotalSize, CostTime: int64(costTime)}
			req := &types.WorkloadRecordReq{WorkloadID: downloadInfo.WorkloadID, AssetCID: dt.req.CID, Workloads: []types.Workload{workload}}
			err = dt.submitWorkload(ctx, req, downloadInfo.SchedulerURL)
			if err != nil {
				return fmt.Errorf("sumbitWorkload failed: %s", err.Error())
			}

			return nil
		}
	}

	return nil
}

func (dt *downloadingTask) doDownloadFile(ctx context.Context, downloadInfo *types.SourceDownloadInfo) error {
	if dt.httpClient == nil {
		dt.httpClient = client.NewHTTP3Client()
	}
	buf, err := encode(downloadInfo.Tk)
	if err != nil {
		return fmt.Errorf("encode %s", err.Error())
	}

	log.Infof("doDownloadFile %s %s", dt.req.CID, dt.req.DownloadPath)

	filename := filepath.Base(dt.req.DownloadPath)
	url := fmt.Sprintf("https://%s/ipfs/%s?filename=%s&download=true", downloadInfo.Address, dt.req.CID, filename)

	req, err := http.NewRequest(http.MethodGet, url, buf)
	if err != nil {
		return fmt.Errorf("newRequest %s", err.Error())
	}
	req = req.WithContext(ctx)

	resp, err := dt.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("doRequest %s", err.Error())
	}
	defer resp.Body.Close() //nolint:errcheck // ignore error

	if resp.StatusCode != http.StatusOK {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("http status code: %d, read body error %s", resp.StatusCode, err.Error())
		}
		return fmt.Errorf("http status code: %d, error msg: %s", resp.StatusCode, string(data))
	}

	dt.progress.TotalSize = resp.ContentLength
	progressReader := newProgressReader(resp.Body, func(doneSize int64) {
		dt.progress.DoneSize = doneSize
	})

	templateFile := filepath.Join(filepath.Dir(dt.req.DownloadPath), dt.req.CID)
	defer removeTemplateFileIfExist(templateFile)

	file, err := os.Create(templateFile)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, progressReader)
	if err != nil {
		return err
	}

	file.Close()
	if err = os.Rename(templateFile, dt.req.DownloadPath); err != nil {
		return err
	}

	return nil
}

func removeTemplateFileIfExist(filePath string) {
	if _, err := os.Stat(filePath); err == nil {
		if err = os.Remove(filePath); err != nil {
			log.Errorf("remove template file error ", err.Error())
		}
	}
}

func (dt *downloadingTask) submitWorkload(ctx context.Context, workload *types.WorkloadRecordReq, schedulerURL string) error {
	schedulerAPI, close, err := client.NewScheduler(ctx, schedulerURL, nil, jsonrpc.WithHTTPClient(dt.httpClient))
	if err != nil {
		return err
	}
	defer close()

	return schedulerAPI.SubmitWorkloadReportV2(ctx, workload)
}

func encode(esc *types.Token) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(esc)
	if err != nil {
		return nil, err
	}

	return &buffer, nil
}

type ProgressReader struct {
	r        io.Reader
	doneSize int64
	callback func(doneSize int64)
}

func newProgressReader(r io.Reader, callback func(doneSize int64)) *ProgressReader {
	return &ProgressReader{
		r:        r,
		callback: callback,
	}
}

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	if err != nil {
		return
	}

	pr.doneSize += int64(n)
	pr.callback(pr.doneSize)
	return
}
