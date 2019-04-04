package rpc

import (
	"net"
	"strings"

	lru "github.com/hashicorp/golang-lru"
)

var (
	cache           *lru.Cache
	CacheSize       = 128
	MapKeySeparator = []byte{255}
)

func GetComputedResult(addr net.Addr, req []byte) ([]byte, bool) {
	// create cache if not initialized yet
	if cache == nil {
		var err error
		cache, err = lru.New(CacheSize)
		if err != nil {
			panic(err)
		}
	}

	key := combineAddrSeqID(addr, req)

	if val, ok := cache.Get(key); ok {
		return val.([]byte), true
	}
	return nil, false
}

func PutComputedResult(addr net.Addr, req []byte, result []byte) {
	// create cache if not initialized yet
	if cache == nil {
		var err error
		cache, err = lru.New(CacheSize)
		if err != nil {
			panic(err)
		}
	}

	key := combineAddrSeqID(addr, req)

	cache.Add(key, result)
}

func combineAddrSeqID(addr net.Addr, req []byte) string {
	strBuilder := new(strings.Builder)

	strBuilder.WriteString(addr.String())
	strBuilder.WriteString(string(MapKeySeparator))
	strBuilder.WriteString(string(req))

	return strBuilder.String()
}
