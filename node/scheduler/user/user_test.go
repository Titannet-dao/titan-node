package user

import (
	"context"
	"fmt"
	"testing"

	"github.com/Filecoin-Titan/titan/api/client"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/common"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/scheduler/db"
	"github.com/Filecoin-Titan/titan/node/sqldb"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gbrlsnchs/jwt/v3"
)

func TestUserAllocateStorage(t *testing.T) {
	sqldb, err := sqldb.NewDB("user01:sql001@tcp(127.0.0.1:3306)/test")
	if err != nil {
		t.Errorf("NewDB error:%s", err.Error())
		return
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}
	u := User{SQLDB: db, ID: "123"}
	if storageSize, err := u.AllocateStorage(context.Background(), 209715200); err != nil {
		t.Errorf("AllocateStorage error:%s", err.Error())
	} else {
		t.Logf("AllocateStorage totalSize %d, usedSize %d", storageSize.TotalSize, storageSize.UsedSize)
	}

	if storageSize, err := u.LoadUserInfo(u.ID); err != nil {
		t.Errorf("LoadUserInfo error:%s", err.Error())
	} else {
		t.Logf("LoadUserInfo totalSize %d, usedSize %d", storageSize.TotalSize, storageSize.UsedSize)
	}

	if storageSize, err := u.LoadUserInfo(u.ID); err != nil {
		t.Errorf("LoadUserInfo error:%s", err.Error())
	} else {
		t.Logf("LoadUserInfo totalSize %d, usedSize %d", storageSize.TotalSize, storageSize.UsedSize)
	}

	// u.CreateAPIKey(context.Background(), "abc")
}

func TestCandidateConnet(t *testing.T) {
	candidate, close, err := client.NewCandidate(context.TODO(), "https://192.30.242.140:65283/rpc/v0", nil, jsonrpc.WithHTTPClient(client.NewHTTP3Client()))
	if err != nil {
		fmt.Println("NewCandidate ", err)
		return
	}
	defer close()

	v, err := candidate.Version(context.Background())
	if err != nil {
		fmt.Println("Version ", err)
		return
	}

	fmt.Printf("version: %#v\n", v)
}

func TestUserAPIKey(t *testing.T) {
	sqldb, err := sqldb.NewDB("user01:sql001@tcp(127.0.0.1:3306)/test")
	if err != nil {
		t.Errorf("NewDB error:%s", err.Error())
		return
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}

	u := User{SQLDB: db, ID: "123"}
	apiKeys, err := u.GetAPIKeys(context.Background())
	if err != nil {
		t.Errorf("GetAPIKeys error %s", err.Error())
		return
	}

	t.Logf("GetAPIKeys %s", apiKeys)

	if len(apiKeys) > 0 {
		for name, key := range apiKeys {
			if err := u.DeleteAPIKey(context.Background(), name); err != nil {
				t.Errorf("DeleteAPIKey error %s", err.Error())
				return
			} else {
				t.Logf("DeleteAPIKey %s %s", name, key)
			}
		}
	}

	// schedulerAPI := api.Scheduler
	apiKey, err := u.CreateAPIKey(context.Background(), "test_key", types.UserAccessControlAll, config.DefaultSchedulerCfg(), &common.CommonAPI{APISecret: jwt.NewHS256([]byte("abc_123"))})
	if err != nil {
		t.Errorf("CreateAPIKey error %s", err.Error())
		return
	}

	t.Logf("CreateAPIKey %s", apiKey)
}

func TestGetUserAPIKeys(t *testing.T) {
	sqldb, err := sqldb.NewDB("user01:sql001@tcp(127.0.0.1:3306)/test")
	if err != nil {
		t.Errorf("NewDB error:%s", err.Error())
		return
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}

	u := User{SQLDB: db, ID: "123"}
	apiKeys, err := u.GetAPIKeys(context.Background())
	if err != nil {
		t.Errorf("GetAPIKeys error %s", err.Error())
		return
	}

	t.Logf("GetAPIKeys %s", apiKeys)
}

func TestGetAssetName(t *testing.T) {
	sqldb, err := sqldb.NewDB("user01:sql001@tcp(127.0.0.1:3306)/test")
	if err != nil {
		t.Errorf("NewDB error:%s", err.Error())
		return
	}

	db, err := db.NewSQLDB(sqldb)
	if err != nil {
		t.Errorf("NewSQLDB error:%s", err.Error())
		return
	}

	u := User{SQLDB: db, ID: "123"}
	name, err := u.GetAssetName("12200c6579f02e720c35c9580ab6c3a991e322f70844e04d5a90ce7add4dfcb3c343", "123")
	if err != nil {
		t.Errorf("GetAPIKeys error %s", err.Error())
		return
	}

	t.Logf("name %s", name)
}
