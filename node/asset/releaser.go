package asset

import (
	"sync"
	"time"

	"github.com/Filecoin-Titan/titan/api/types"
)

const releaseTimeout = 60 * time.Second

// releaser contains releasing files and error info
type releaser struct {
	*sync.Mutex
	files    map[string]*releaserState
	nextTime int64
}

type releaserState struct {
	errMsg string
	// done   bool
}

func NewReleaser() *releaser {
	return &releaser{
		Mutex: &sync.Mutex{},
		files: make(map[string]*releaserState),
	}
}

func (r *releaser) clear() {
	r.Lock()
	defer r.Unlock()
	r.files = make(map[string]*releaserState)
}

func (r *releaser) count() int {
	r.Lock()
	defer r.Unlock()
	return len(r.files)
}

func (r *releaser) initFiles(hashes []string) {
	r.Lock()
	defer r.Unlock()
	r.files = make(map[string]*releaserState)
	for _, hash := range hashes {
		r.files[hash] = &releaserState{}
	}

	go r.clearOnTimeout(time.Now().Add(releaseTimeout))
}

func (r *releaser) setNextTime(time int64) {
	r.Lock()
	defer r.Unlock()
	r.nextTime = time
}

func (r *releaser) clearOnTimeout(rt time.Time) {
	time.AfterFunc(time.Until(rt), func() {
		log.Warnf("releaser: timeout on clearing files: %v", r.files)
		r.clear()
	})
}

func (r *releaser) setFile(hash string, err error) {
	state, ok := r.files[hash]
	if !ok {
		log.Errorf("cannot find hash:%s in relase map: %v", hash, r.files)
		return
	}

	r.Lock()
	defer r.Unlock()
	if err != nil {
		state.errMsg = err.Error()
	} else {
		delete(r.files, hash)
	}
}

func (r *releaser) load() (ret []*types.FreeUpDiskState) {
	for hash, v := range r.files {
		ret = append(ret, &types.FreeUpDiskState{
			Hash:   hash,
			ErrMsg: v.errMsg,
		})
	}
	return ret
}
