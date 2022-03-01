package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/dustin/go-humanize"
	"github.com/gagliardetto/messari-challenge/stdoutinator/models"
	. "github.com/gagliardetto/utilz"
	"github.com/hako/durafmt"
	jsoniter "github.com/json-iterator/go"
)

// TODO:
// - accept input from stdin
// - process after BEGIN
// - exit after END
// - ignore if NOT json
// - on exit: print results as newline-delimited json objects.
// - one aggregate result per market (spanning all provided data)
// - result data:
// 		- Total volume per market
// 		- Mean price per market
// 		- Mean volume per market
// 		- Volume-weighted average price per market
// 		- Percentage buy orders per market
// - Example (print one of these for each market across all provided data):
// {
//     "market": 5775,
//     "total_volume": 1234567.89,
//     "mean_price": 23.33,
//     "mean_volume": 6144.299,
//     "vwap": 5234.2,
//     "percentage_buy": 0.50
// }

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var BEGIN = []byte("BEGIN\n")
var END = []byte("END\n")

func main() {
	took := NewTimerRaw()

	numTrades := uint64(0)
	defer func() {
		// Before exiting, print stats to stderr:
		dur := took()
		fmt.Fprintf(
			os.Stderr,
			"Took %s for processing %v trades (%s TPS)\n",
			durafmt.Parse(dur),
			humanize.Comma(int64(numTrades)),
			humanize.CommafWithDigits(float64(numTrades)/dur.Seconds(), 2),
		)
	}()

	ag := NewAggregator()

	// Iterate over input:
	err := iterateLines(
		os.Stdin,
		func(line []byte) bool {
			if line[0] != '{' {
				if bytes.Equal(line, BEGIN[:]) {
					return true
				}
				if bytes.Equal(line, END[:]) {
					return false
				}
				fmt.Fprintf(
					os.Stderr,
					"%s",
					string(line),
				)
				return true
			}
			atomic.AddUint64(&numTrades, 1)

			// Parse trade:
			var trade models.Trade
			if err := json.Unmarshal(line, &trade); err != nil {
				panic(err)
			}
			// Get market:
			mkt := ag.GetMarket(trade.Market)

			// Process trade data for the market:
			mkt.Lock(func(mkt *Market) {
				mkt.numTrades++

				mkt.totalVolume += trade.Volume
				mkt.totalPrice += trade.Price
				mkt.priceXvolumeSum += trade.Price * trade.Volume

				if trade.IsBuy {
					mkt.numBuy++
				}
			})
			return true
		},
	)
	if err != nil {
		panic(err)
	}

	// Compute results:
	computed := ag.Compute()

	// Print results:
	for _, mc := range computed {
		res, err := json.MarshalToString(mc)
		if err != nil {
			panic(err)
		}
		Ln(res)
	}
}

func NewAggregator() *Markets {
	return &Markets{
		mu:     sync.RWMutex{},
		mapper: map[int]*Market{},
	}
}

type Market struct {
	mu sync.Mutex

	totalVolume float64
	totalPrice  float64

	numBuy    int
	numTrades int

	priceXvolumeSum float64
}

type Markets struct {
	mu     sync.RWMutex
	mapper map[int]*Market
}

func NewMarket() *Market {
	return &Market{}
}

func (ag *Markets) GetMarket(id int) *Market {
	ag.mu.RLock()
	got, ok := ag.mapper[id]
	ag.mu.RUnlock()
	if !ok {
		ag.mu.Lock()
		created := NewMarket()
		ag.mapper[id] = created
		got = created
		ag.mu.Unlock()
	}
	return got
}

type M map[string]interface{}

func (ag *Markets) Compute() []M {
	out := make([]M, 0)
	for id, mkt := range ag.mapper {
		out = append(out,
			M{
				"market":         id,
				"total_volume":   mkt.totalVolume,
				"mean_volume":    mkt.totalVolume / float64(mkt.numTrades),
				"mean_price":     mkt.totalPrice / float64(mkt.numTrades),
				"percentage_buy": GetPercent(int64(mkt.numBuy), int64(mkt.numTrades)), // 0.00 - 100.00 %
				"vwap":           mkt.priceXvolumeSum / mkt.totalVolume,
			},
		)
	}
	return out
}

func (mkt *Market) Lock(f func(*Market)) {
	mkt.mu.Lock()
	defer mkt.mu.Unlock()
	f(mkt)
}

func iterateLines(source io.Reader, iterator func(b []byte) bool) error {

	reader := bufio.NewReader(source)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("error of reader: %s", err)
			}
			break
		}
		doContinue := iterator(line)
		if !doContinue {
			return nil
		}
	}

	return nil
}
