package distributor

import (
	"sync"

	"github.com/coreos/pkg/capnslog"
)

// cache implements an LRU cache.
type cache struct {
	cache     map[string]int
	priority  []string
	maxSize   int
	blockSize int
	mut       sync.Mutex
	data      []byte
}

func newCache(size int, blocksize int) *cache {
	var lru cache
	lru.maxSize = size
	lru.blockSize = blocksize
	lru.priority = make([]string, 0)
	lru.cache = make(map[string]int)
	lru.data = make([]byte, lru.maxSize*lru.blockSize)
	return &lru
}

func (lru *cache) Put(key string, value []byte) {
	if lru == nil {
		return
	}
	lru.mut.Lock()
	defer lru.mut.Unlock()
	if v, ok := lru.cache[key]; ok {
		clog.Warningf("Caching the same block twice? block: %s", key)
		off := v * lru.blockSize
		copy(lru.data[off:], value)
		// move to top
		for i := 0; i < len(lru.priority); i++ {
			if lru.priority[i] == key {
				copy(lru.priority[1:], lru.priority[:i])
				lru.priority[0] = key
				return
			}
		}
		panic("couldn't find key in priority list")
	}
	var slot int
	if len(lru.priority) >= lru.maxSize {
		slot = lru.removeOldest()
	} else {
		slot = len(lru.priority)
	}
	off := slot * lru.blockSize
	lru.priority = append([]string{key}, lru.priority...)
	copy(lru.data[off:], value)
	lru.cache[key] = slot
	if clog.LevelAt(capnslog.TRACE) {
		clog.Infof("putting %s: %d:%d", key, len(lru.cache), slot)
	}
}

func (lru *cache) Get(key string) ([]byte, bool) {
	if lru == nil {
		return nil, false
	}
	lru.mut.Lock()
	defer lru.mut.Unlock()
	v, ok := lru.cache[key]
	if !ok {
		return nil, false
	}
	if clog.LevelAt(capnslog.TRACE) {
		clog.Tracef("found %s: %d:%d", key, len(lru.cache), v)
	}
	off := v * lru.blockSize
	return lru.data[off : off+lru.blockSize], true
}

func (lru *cache) removeOldest() int {
	last := lru.priority[len(lru.priority)-1]
	lru.priority = lru.priority[:len(lru.priority)-1]
	free := lru.cache[last]
	delete(lru.cache, last)
	return free
}
