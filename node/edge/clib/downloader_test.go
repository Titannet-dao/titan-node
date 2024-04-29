package clib

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
)

func TestDownloader(t *testing.T) {
	t.Log("TestDownloader")

	logging.SetAllLoggers(logging.LevelDebug)
	testTask()
	// testTetDownloadInfos()
	// testGetCandidateDownloadInfo()
}

func testTask() {
	req := DownloadFileReq{CID: "bafybeidbxvqnrak6tbnyaqg4nwzwgqm4yhrfyie7kwpmnz7hiqiczo3w6e", DownloadPath: "d:/filecoin-titan/test2/830_2533.pdf", LocatorURL: "https://test-locator.titannet.io:5000/rpc/v0"}

	d := newDownloader()
	if err := d.downloadFile(&req); err != nil {
		fmt.Println("downloadFile error ", err.Error())
	}

	go checkProgress(d)

	time.Sleep(1 * time.Minute)
}

func checkProgress(d *Downloader) {
	for {
		result, err := d.queryProgress("d:/filecoin-titan/test2/830_2533.pdf")
		if err != nil {
			fmt.Println("err ", err.Error())
			return
		}

		time.Sleep(3 * time.Second)

		fmt.Println("progress ", *result)
	}
}

func testTetDownloadInfos() {
	req := DownloadFileReq{CID: "bafybeifaxtfwqqqoxzjvpfra3rsrq46v4vkl7kjoclc7mv4pjy7y5asv24", DownloadPath: "./test/abc.pdf", LocatorURL: "https://test-locator.titannet.io:5000/rpc/v0"}
	task := downloadingTask{id: uuid.NewString(), req: &req, progress: &Progress{}, httpClient: client.NewHTTP3Client()}
	infos, err := task.getDownloadInfos(context.Background())
	if err != nil {
		fmt.Println("get download info error ", err.Error())
		return
	}

	fmt.Println("info len: ", len(infos))
}

func testGetCandidateDownloadInfo() {
	token := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJ3ZWIiXSwiSUQiOiI4Nzk1OTg3Ni1iZTdmLTRlYTctOGE2NS1jZGU4OWQzMzA1ZGIiLCJOb2RlSUQiOiIiLCJFeHRlbmQiOiIiLCJBY2Nlc3NDb250cm9sTGlzdCI6bnVsbH0.8r47dXtbgNpa1ZG34BMlgaXIbYqKlUGviuY50BchDDY"
	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	// headers.Add("Node-ID", nodeID)

	url := "http://test4-scheduler.titannet.io:3456/rpc/v0"
	scheduler, close, err := client.NewScheduler(context.Background(), url, headers)
	if err != nil {
		fmt.Println("new scheduler error ", err.Error())
		return
	}
	defer close()

	cid := "bafybeidbxvqnrak6tbnyaqg4nwzwgqm4yhrfyie7kwpmnz7hiqiczo3w6e"
	infos, err := scheduler.GetCandidateDownloadInfos(context.Background(), cid)
	if err != nil {
		fmt.Println("GetCandidateDownloadInfos error ", err.Error())
		return
	}

	fmt.Println("infos len ", len(infos))
}
