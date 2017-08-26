package main

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Trades []struct {
	Tid    int
	Amount float64
	Price  float64
	Date   int
}

func main() {
	log.SetOutput(os.Stdout)

	// Catch signals so that kv.Close is called.
	sigs := make(chan os.Signal, 1)
	done := false
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		fmt.Println()
		log.Println(sig)
		done = true
	}()

	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <data directory>\n", os.Args[0])
		os.Exit(1)
	}

	// Open existing badger key-value store, or create if it doesn't exist.
	opts := badger.DefaultOptions
	opts.Dir = os.Args[1]
	opts.ValueDir = os.Args[1]
	kv, err := badger.NewKV(&opts)
	if err != nil {
		panic(err)
	}
	defer kv.Close()

	// Count existing items, find latest trade id.
	itr := kv.NewIterator(badger.IteratorOptions{
		PrefetchSize: 100,
		FetchValues:  false,
		Reverse:      false,
	})
	max_tid := 0
	key_count := 0
	max_date := ""
	for itr.Rewind(); itr.Valid(); itr.Next() {
		item := itr.Item()
		key := item.Key()
		fields := strings.Fields(string(key))
		tid, err := strconv.Atoi(fields[1])
		if err != nil {
			panic(err)
		}
		if tid > max_tid {
			max_tid = tid
			t, err := strconv.ParseInt(fields[0], 10, 64)
			if err != nil {
				panic(err)
			}
			max_date = fmt.Sprintf("%v", (time.Unix(t, 0)))
		}
		key_count++
	}

	if key_count != 0 {
		log.Printf("Found %d previously logged trades, latest at %s.\n", key_count, max_date)
	}
	for !done {
		url := fmt.Sprintf("https://api.btcmarkets.net/market/BTC/AUD/trades?since=%d", max_tid)
		resp, err := http.Get(url)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		var t Trades
		err = json.Unmarshal(body, &t)
		if err != nil {
			panic(err)
		}

		if len(t) > 0 {
			log.Printf("Processing %d trade(s)\n", len(t))
			for _, trade := range t {
				data, err := json.Marshal(trade)
				if err != nil {
					panic(err)
				}
				err = kv.Set([]byte(fmt.Sprintf("%d %d", trade.Date, trade.Tid)), data, 0x00)
				if err != nil {
					panic(err)
				}
				if trade.Tid > max_tid {
					max_tid = trade.Tid
				}
			}
		}
		time.Sleep(time.Second)
	}
}
