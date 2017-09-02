package main

import (
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger"
	bb "github.com/shric/btcmarkets/badger"
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

	kv := bb.OpenBadger(os.Args[1])
	defer kv.Close()

	max_tids := getMaxTids(kv)

	trading_pairs := []string{
		"BTC/AUD", "LTC/AUD", "ETH/AUD", "ETC/AUD", "XRP/AUD", "BCH/AUD",
		"LTC/BTC", "ETH/BTC", "ETC/BTC", "XRP/BTC", "BCH/BTC"}
	for !done {
		for _, trading_pair := range trading_pairs {
			url := fmt.Sprintf("https://api.btcmarkets.net/market/%s/trades?since=%d",
				trading_pair, max_tids[trading_pair])
			timeout := time.Duration(5 * time.Second)
			client := http.Client{
				Timeout: timeout,
			}
			resp, err := client.Get(url)
			if err != nil {
				log.Println(err)
				time.Sleep(time.Second)
				continue
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			var t Trades
			err = json.Unmarshal(body, &t)
			if err != nil {
				log.Println(err)
				continue
			}

			if len(t) > 0 {
				log.Printf("%s: Processing %d trade(s)\n", trading_pair, len(t))
				for _, trade := range t {
					data, err := json.Marshal(trade)
					if err != nil {
						panic(err)
					}
					err = kv.Set([]byte(fmt.Sprintf("%s %d %d",
						trading_pair, trade.Date, trade.Tid)), data, 0x00)
					if err != nil {
						panic(err)
					}
					if trade.Tid > max_tids[trading_pair] {
						max_tids[trading_pair] = trade.Tid
					}
				}
			}
			time.Sleep(500 * time.Millisecond)
			if done {
				break
			}
		}
	}
}

func getMaxTids(kv *badger.KV) map[string]int {
	// Count existing items, find latest trade id.
	itr := kv.NewIterator(badger.IteratorOptions{
		PrefetchSize: 100,
		FetchValues:  false,
		Reverse:      false,
	})
	var max_tids map[string]int
	max_tids = make(map[string]int)
	key_count := 0
	max_date := ""
	for itr.Rewind(); itr.Valid(); itr.Next() {
		item := itr.Item()
		key := item.Key()
		fields := strings.Fields(string(key))
		tid, err := strconv.Atoi(fields[2])
		trading_pair := fields[0]
		if err != nil {
			panic(err)
		}
		if tid > max_tids[trading_pair] {
			max_tids[trading_pair] = tid
			t, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				panic(err)
			}
			max_date = fmt.Sprintf("%v", (time.Unix(t, 0)))
		}
		key_count++
	}

	if key_count != 0 {
		log.Printf("Found %d previously logged trades, latest at %s.\n",
			key_count, max_date)
	}
	return max_tids
}
