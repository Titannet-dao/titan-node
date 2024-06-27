package cidutil

import (
	"testing"
	"time"
)

func TestCidToHash(t *testing.T) {
	cid := "QmVdnEzsQHcSqebVosHiEMFoonMrUGZDzwCgVVBMEQPhN2"

	hash, err := CIDToHash(cid)
	if err != nil {
		t.Errorf("%s CIDToHash error:%s", cid, err.Error())
		return
	}

	cid, err = HashToCID(hash)
	if err != nil {
		t.Errorf("%s CIDToHash error:%s", cid, err.Error())
		return
	}

	t.Logf("%s CIDToHash: %s", cid, hash)
}

func TestTime(t *testing.T) {
	it, _ := time.Parse("2006-01-02 15:04:05", "2024-04-20 12:10:15")
	day := 5
	now := time.Now()
	fiveDaysAgo := now.AddDate(0, 0, -day)

	t.Logf(" TestTime: %v", it.Before(fiveDaysAgo))
}

type ProfitDetails struct {
	ID          int64     `db:"id"`
	NodeID      string    `db:"node_id"`
	Profit      float64   `db:"profit"`
	CreatedTime time.Time `db:"created_time"`
	Size        int64     `db:"size"`
	Note        string    `db:"note"`
	CID         string    `db:"cid"`
}

func TestXxx(t *testing.T) {
	detailsList := make([]*ProfitDetails, 0)

	for _, info := range detailsList {
		t.Log(info)
	}
}
