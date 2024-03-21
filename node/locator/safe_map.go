package locator

import "sync"

// It differs from sync.map by the addition of the len() function
// sync.map can not delete item on range
type SafeMap struct {
	mu sync.Mutex
	m  map[interface{}]interface{}
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[interface{}]interface{}),
	}
}

func (sm *SafeMap) Set(key interface{}, value interface{}) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

func (sm *SafeMap) Get(key interface{}) (interface{}, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	value, ok := sm.m[key]
	return value, ok
}

func (sm *SafeMap) Delete(key interface{}) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.m, key)
}

func (sm *SafeMap) Len() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.m)
}

// can not call any SafeMap func in callback, it will block anything
func (sm *SafeMap) Range(callback func(key interface{}, value interface{})) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for key, value := range sm.m {
		callback(key, value)
	}
}
