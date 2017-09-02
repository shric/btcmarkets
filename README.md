# btcmarkets-scraper
Scrapes [BTC Markets](https://btcmarkets.net) BTC/AUD trades using its
[API](https://github.com/BTCMarkets/API) and stores them in a
[badger](https://github.com/dgraph-io/badger) key-value store.

When the program is restarted, it will find the most recent trade stored and ask
BTC Markets API for any trades that have occurred since, continuing where it
left off.

### Quick Usage

- badger appears to require Go 1.8 or higher.
- Ensure you have a correct GOPATH environment variable (points to where you install Go libraries for go get below).
- $ go get github.com/dgraph-io/badger
- $ mkdir /path/to/kv/directory
- $ go run main/btcmarkets-scraper.go /path/to/kv/directory



### TODO

- [ ] Add support for trading pairs other than BTC/AUD.
