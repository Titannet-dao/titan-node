package storage

import (
	"context"
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/ipfs/go-cid"
	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"bytes"
	"io"

	"github.com/ipfs/go-unixfsnode/data/builder"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

const (
	timeInterval      = 3 * time.Second
	eventObjectCreate = "s3:ObjectCreated:*"
)

type IMinioService interface {
	GetMinioStat(ctx context.Context) (totalSpace, usage float64)
}

type minioService struct {
	adminClient  *madmin.AdminClient
	s3Client     *minio.Client
	schedulerAPI api.Scheduler
	objects      []*object
	lock         *sync.Mutex
}

type object struct {
	name   string
	bucket string
}

func newMinioService(config *config.MinioConfig, schedulerAPI api.Scheduler) (IMinioService, error) {
	if len(config.AccessKeyID) == 0 || len(config.Endpoint) == 0 || len(config.SecretAccessKey) == 0 {
		return nil, nil
	}
	// Initialize minio client object.
	adminClient, err := madmin.New(config.Endpoint, config.AccessKeyID, config.SecretAccessKey, false)
	if err != nil {
		return nil, err
	}

	minClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	client := &minioService{
		adminClient:  adminClient,
		s3Client:     minClient,
		schedulerAPI: schedulerAPI,
		objects:      make([]*object, 0),
		lock:         &sync.Mutex{},
	}

	go client.ListenObjectCreate()
	go client.run()

	return client, nil

}

func (mc *minioService) ListenObjectCreate() {
	log.Debugf("listen minio object create")

	// TODO recovery panic

	for {
		notificationInfos := mc.s3Client.ListenNotification(context.Background(), "", "", []string{
			eventObjectCreate,
		})

		for info := range notificationInfos {
			if info.Err != nil {
				log.Error("ListenNotification error ", info.Err.Error())
				continue
			}

			for _, event := range info.Records {
				mc.addObject(&object{bucket: event.S3.Bucket.Name, name: event.S3.Object.Key})
				log.Debugf("listen event %s, bucket %s, object %s,", event.EventName, event.S3.Bucket.Name, event.S3.Object.Key)
			}
		}
	}

}

func (mc *minioService) run() {
	for {
		time.Sleep(timeInterval)

		for object := mc.nextObject(); object != nil; {
			if err := mc.calculateCidAndSend(object.bucket, object.name); err != nil {
				log.Errorf("calculateCidAndSend error %s", err.Error())
			}

			object = mc.nextObject()
		}
	}
}

func (mc *minioService) nextObject() *object {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	if len(mc.objects) == 0 {
		return nil
	}

	obj := mc.objects[0]
	mc.objects = mc.objects[1:]
	return obj
}

func (mc *minioService) addObject(obj *object) {
	mc.lock.Lock()
	defer mc.lock.Unlock()
	mc.objects = append(mc.objects, obj)
}

func (mc *minioService) calculateCidAndSend(bucket, object string) error {
	obj, err := mc.s3Client.GetObject(context.Background(), bucket, object, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer obj.Close()

	cid, err := mc.calculateCid(obj)
	if err != nil {
		return err
	}

	objInfo, err := mc.s3Client.StatObject(context.Background(), bucket, object, minio.StatObjectOptions{})
	if err != nil {
		return err
	}

	event := &types.MinioUploadFileEvent{
		AssetCID:   cid.String(),
		Size:       objInfo.Size,
		CreateTime: objInfo.LastModified,
		Expiration: objInfo.Expiration,
	}

	if event.Expiration.IsZero() {
		event.Expiration = time.Now().AddDate(20, 0, 0)
	}

	log.Debugf("cid %s, size %d, CreateTime %s, Expiration:%s", cid.String(), event.Size, event.CreateTime, event.Expiration)
	return mc.schedulerAPI.MinioUploadFileEvent(context.Background(), event)
}

func (mc *minioService) calculateCid(r io.Reader) (cid.Cid, error) {
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true

	ls.StorageReadOpener = func(_ ipld.LinkContext, l ipld.Link) (io.Reader, error) {
		return nil, nil
	}

	ls.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(l ipld.Link) error {
			return nil
		}, nil
	}

	link, _, err := builder.BuildUnixFSFile(r, "", &ls)
	if err != nil {
		return cid.Cid{}, err
	}

	root := link.(cidlink.Link)
	return root.Cid, nil
}

func (mc *minioService) GetMinioStat(ctx context.Context) (totalSpace, usage float64) {
	info, err := mc.adminClient.ServerInfo(ctx)
	if err != nil {
		log.Errorf("minio admin get server info error %s", err.Error())
		return 0, 0
	}

	rawCapacity := uint64(0)
	rawUsage := uint64(0)
	for _, pool := range info.Pools {
		for _, erasureSet := range pool {
			rawCapacity += erasureSet.RawCapacity
			rawUsage += erasureSet.RawUsage
		}
	}

	return float64(rawCapacity), float64(rawUsage) / float64(rawCapacity)
}
