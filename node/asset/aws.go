package asset

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/lib/limiter"
	"github.com/Filecoin-Titan/titan/node/asset/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"golang.org/x/time/rate"
)

const defaultRegion = "us-east-1"

type AWS interface {
	// PullAssetWithURL download the file locally from the url and save it as car file
	PullAssetFromAWS(ctx context.Context, bucket, key string) error
}

func NewAWS(scheduler api.Scheduler, storage storage.Storage, rateLimiter *rate.Limiter) AWS {
	return &awsClient{
		scheduler:   scheduler,
		storage:     storage,
		rateLimiter: rateLimiter,
	}
}

func newS3(region string) *s3.S3 {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	return s3.New(sess, &aws.Config{Credentials: credentials.AnonymousCredentials})
}

func getRegion(bucket string) (string, error) {
	parts := strings.SplitN(bucket, "/", 2)
	if len(parts) > 1 {
		bucket = parts[0]
	}
	sess := session.Must(session.NewSession())
	return s3manager.GetBucketRegion(context.Background(), sess, bucket, defaultRegion)
}

type awsClient struct {
	isRunning   bool
	object      awsObject
	scheduler   api.Scheduler
	storage     storage.Storage
	rateLimiter *rate.Limiter
}

type awsObject struct {
	bucket string
	key    string
}

type bucketSummary struct {
	bucket    string
	keys      []string
	totalSize int64
}

// PullAssetWithURL download the file locally from the url and save it as car file
func (ac *awsClient) PullAssetFromAWS(ctx context.Context, bucket, key string) error {
	if ac.isRunning {
		return fmt.Errorf("downloading bucket %s key %s", ac.object.bucket, ac.object.key)
	}

	ac.isRunning = true
	ac.object = awsObject{bucket: bucket, key: key}

	go func() {
		defer func() {
			ac.isRunning = false
		}()

		ctx := context.Background()
		cid, size, err := ac.pullAssetFromAWS(ctx, bucket, key)
		if err != nil {
			log.Errorln("pullAssetFromAWS error ", err.Error())
		}

		var cidString string
		if cid.Defined() {
			cidString = cid.String()
		}

		if err := ac.scheduler.DownloadDataResult(ctx, bucket, cidString, size); err != nil {
			// if call scheduler failed, remove the asset
			if cid.Defined() {
				if err = ac.storage.DeleteAsset(cid); err != nil {
					log.Errorln("DownloadDataResult failed, deleter asset error ", err.Error())
				}
			}
			log.Errorln("DownloadDataResult error ", err.Error())
		}
	}()

	return nil
}

func (ac *awsClient) pullAssetFromAWS(ctx context.Context, bucket, key string) (cid.Cid, int64, error) {
	region, err := getRegion(bucket)
	if err != nil {
		return cid.Cid{}, 0, fmt.Errorf("get bucket[%s] region error %s", bucket, err.Error())
	}

	log.Debugf("get bucket %s region=[%s]", bucket, region)

	svc := newS3(region)

	summary, err := ac.getBucketSummary(ctx, svc, bucket, key)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	if summary.totalSize >= ac.usableDiskSpace() {
		return cid.Cid{}, 0, fmt.Errorf("not enough disk space, need %d, usable %d, bucket %s", summary.totalSize, ac.usableDiskSpace(), bucket)
	}

	log.Debugf("bucket %s size %d, keys len %d", summary.bucket, summary.totalSize, len(summary.keys))

	assetDir, err := ac.storage.AllocatePathWithSize(summary.totalSize)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	assetTempDirPath := path.Join(assetDir, uuid.NewString())
	if err = os.Mkdir(assetTempDirPath, 0755); err != nil {
		return cid.Cid{}, 0, err
	}
	defer os.RemoveAll(assetTempDirPath)

	if err := ac.downloadObjects(ctx, svc, summary, assetTempDirPath); err != nil {
		return cid.Cid{}, 0, err
	}

	tempCarFile := path.Join(assetDir, uuid.NewString())
	rootCID, err := createCar(assetTempDirPath, tempCarFile)
	if err != nil {
		return cid.Cid{}, 0, err
	}
	defer os.RemoveAll(tempCarFile)

	fInfo, err := os.Stat(tempCarFile)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	var isExists bool
	if isExists, err = ac.storage.AssetExists(rootCID); err != nil {
		return cid.Cid{}, 0, err
	} else if isExists {
		log.Debugf("asset %s already exist", rootCID.String())
		return rootCID, fInfo.Size(), nil
	}

	if err = ac.saveCarFile(ctx, tempCarFile, rootCID); err != nil {
		return cid.Cid{}, 0, err
	}

	log.Debugf("downloaded bucket=[%s] key=[%s], cid=[%s]", bucket, key, rootCID.String())
	return rootCID, fInfo.Size(), nil
}

func (ac *awsClient) saveCarFile(ctx context.Context, tempCarFile string, root cid.Cid) error {
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
	if err := ac.storage.StoreUserAsset(ctx, uuid.NewString(), root, fInfo.Size(), f); err != nil {
		return err
	}
	return nil
}

func (ac *awsClient) getBucketSummary(ctx context.Context, svc *s3.S3, bucket, key string) (*bucketSummary, error) {
	var prefix string
	var parts = strings.SplitN(strings.TrimSpace(bucket), "/", 2)
	if len(parts) > 1 {
		bucket = parts[0]
		prefix = parts[1]
	}

	if len(key) > 0 {
		prefix = strings.TrimSuffix(prefix, "/")
		prefix = fmt.Sprintf("%s/%s", prefix, key)
	}

	log.Debugf("getBucketSummary bucket=[%s], prefix=[%s]", bucket, prefix)

	var totalSize = int64(0)
	var objectKeys = make([]string, 0)
	var listObjects = &s3.ListObjectsInput{Bucket: &bucket, Prefix: &prefix}
	err := svc.ListObjectsPages(listObjects, func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
		for _, obj := range p.Contents {
			objectKeys = append(objectKeys, *obj.Key)
			totalSize += aws.Int64Value(obj.Size)
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	if len(objectKeys) == 0 {
		return nil, fmt.Errorf("no key in bucket %s", bucket)
	}

	return &bucketSummary{bucket: bucket, keys: objectKeys, totalSize: totalSize}, nil

}

func (ac *awsClient) downloadObjects(ctx context.Context, svc *s3.S3, summary *bucketSummary, assetsTempDirPath string) error {
	for _, key := range summary.keys {
		filePath := path.Join(assetsTempDirPath, strings.ReplaceAll(key, "/", "-"))
		if err := ac.downloadObject(ctx, svc, summary.bucket, key, filePath); err != nil {
			return err
		}
	}

	return nil
}

func (ac *awsClient) downloadObject(ctx context.Context, svc *s3.S3, bucket, key string, tempFilePath string) error {
	log.Debugf("downloadObject bucket=[%s], key[%s]", bucket, key)

	start := time.Now()

	result, err := svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		fmt.Println("get object error %s", err.Error())
		return err
	}
	defer result.Body.Close()

	if aws.Int64Value(result.ContentLength) >= ac.usableDiskSpace() {
		return fmt.Errorf("not enough disk space, bucket %s", bucket)
	}

	file, err := os.Create(tempFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := limiter.NewReader(result.Body, ac.rateLimiter)
	_, err = io.Copy(file, reader)
	if err != nil {
		return err
	}

	cost := time.Since(start)
	speed := float64(aws.Int64Value(result.ContentLength)) / float64(cost) * float64(time.Second)
	log.Infof("downloadObject complete bucket=[%s], key[%s] size %d cost %dms speed %.2f/s", bucket, key, *result.ContentLength, cost/time.Millisecond, speed)

	return nil
}

func (ac *awsClient) usableDiskSpace() int64 {
	totalSpace, usage := ac.storage.GetDiskUsageStat()
	usable := totalSpace - (totalSpace * (usage / float64(100)))
	return int64(usable)
}
