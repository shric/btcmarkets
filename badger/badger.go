package badger

import (
	"github.com/dgraph-io/badger"
)

func OpenBadger(dir string) *badger.KV {
	// Open existing badger key-value store, or create if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	kv, err := badger.NewKV(&opts)
	if err != nil {
		panic(err)
	}
  return kv
}
