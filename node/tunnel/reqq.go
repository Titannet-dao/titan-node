package tunnel

import (
	"fmt"
	"sync"
)

type Reqq struct {
	requests []*Request
	lock     sync.Mutex
}

func newReqq(cap int) *Reqq {
	requests := make([]*Request, 0, cap)
	for i := 0; i < cap; i++ {
		requests = append(requests, newRequest(uint16(i)))
	}

	return &Reqq{requests: requests, lock: sync.Mutex{}}
}

// func (r *Reqq) reqValid(idx, tag uint16) bool {
// 	if idx < 0 || idx >= uint16(len(r.requests)) {
// 		return false
// 	}

// 	req := r.requests[idx]
// 	if req.tag != tag {
// 		return false
// 	}

// 	return true
// }

func (r *Reqq) getReq(idx, tag uint16) *Request {
	if idx < 0 || int(idx) > len(r.requests) {
		log.Errorf("getReq idx %d, requests len %d", idx, len(r.requests))
		return nil
	}

	req := r.requests[idx]
	if !req.inused {
		log.Errorf("getReq idx %d tag %d unuse", idx, tag)
		return nil
	}

	if req.tag != tag {
		log.Errorf("getReq idx %d req.tag %d != %d", idx, req.tag, tag)
		return nil
	}

	return req
}

func (r *Reqq) allocReq() *Request {
	// log.Infof("allocReq")
	r.lock.Lock()
	defer r.lock.Unlock()

	for _, request := range r.requests {
		if !request.inused {
			request.inused = true
			return request
		}
	}
	return nil
}

func (r *Reqq) free(idx uint16, tag uint16) error {
	if idx >= uint16(len(r.requests)) {
		return fmt.Errorf("free, idx %d >= len %d", idx, len(r.requests))
	}

	req := r.requests[idx]
	if !req.inused {
		return fmt.Errorf("free, req %d:%d is in not used", idx, tag)
	}

	if req.tag != tag {
		return fmt.Errorf("free, req %d:%d is in not match tag %d", idx, tag, req.tag)
	}

	// log.Printf("reqq free req %d:%d", idx, tag)

	req.dofree()
	req.tag++
	req.inused = false

	return nil
}

func (r *Reqq) cleanup() {
	for _, request := range r.requests {
		if request.inused {
			if err := r.free(request.idx, request.tag); err != nil {
				log.Errorf("cleanup free request idx %d tag %d %s", request.idx, request.tag, err.Error())
			}
		}
	}
}
