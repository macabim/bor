package fetcher

import (
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestConfiguredTransactionArrivalWait(t *testing.T) {
	for _, wait := range []time.Duration{100 * time.Millisecond, 500 * time.Millisecond} {
		t.Run(wait.String(), func(t *testing.T) {
			calls := make(chan struct{}, 16)
			testTransactionFetcher(t, txFetcherTest{
				init: func() *TxFetcher {
					return NewTxFetcherWithArrivalWait(func(common.Hash) bool { return false }, nil, func(string, uint64, []common.Hash) error { calls <- struct{}{}; return nil }, nil, wait)
				},
				steps: []interface{}{
					doTxNotify{peer: "A", hashes: []common.Hash{{1}}, types: []byte{types.LegacyTxType}, sizes: []uint32{111}},
					doWait{time: wait - time.Millisecond},
					doFunc(func() {
						if len(calls) != 0 {
							t.Fatal("request before grace expires")
						}
					}),
					doWait{time: time.Millisecond, step: true},
					doFunc(func() {
						select {
						case <-calls:
						case <-time.After(time.Second):
							t.Fatal("no request after grace")
						}
					}),
				},
			})
		})
	}
}

func TestConfiguredArrivalBroadcastCancelsRequest(t *testing.T) {
	for _, wait := range []time.Duration{100 * time.Millisecond, 500 * time.Millisecond} {
		t.Run(wait.String(), func(t *testing.T) {
			calls := make(chan struct{}, 16)
			testTransactionFetcher(t, txFetcherTest{
				init: func() *TxFetcher {
					return NewTxFetcherWithArrivalWait(func(common.Hash) bool { return false }, func(txs []*types.Transaction) []error { return make([]error, len(txs)) }, func(string, uint64, []common.Hash) error { calls <- struct{}{}; return nil }, nil, wait)
				},
				steps: []interface{}{
					doTxNotify{peer: "A", hashes: []common.Hash{testTxs[0].Hash()}, types: []byte{types.LegacyTxType}, sizes: []uint32{uint32(testTxs[0].Size())}},
					doWait{time: wait / 2},
					doTxEnqueue{peer: "A", txs: []*types.Transaction{testTxs[0]}},
					isWaiting(nil),
					doWait{time: wait, step: true},
					doFunc(func() {
						if len(calls) != 0 {
							t.Fatal("request after body broadcast")
						}
					}),
				},
			})
		})
	}
}

func TestArrivalWaitDefaultsAndSlack(t *testing.T) {
	for _, tc := range []struct{ input, want, slack time.Duration }{
		{0, 500 * time.Millisecond, 100 * time.Millisecond},
		{-time.Second, 500 * time.Millisecond, 100 * time.Millisecond},
		{100 * time.Millisecond, 100 * time.Millisecond, 20 * time.Millisecond},
		{500 * time.Millisecond, 500 * time.Millisecond, 100 * time.Millisecond},
		{time.Second, time.Second, 100 * time.Millisecond},
	} {
		f := NewTxFetcherWithArrivalWait(nil, nil, nil, nil, tc.input)
		if f.arrivalWait != tc.want || f.arrivalSlack != tc.slack {
			t.Fatalf("wait %v: got %v/%v", tc.input, f.arrivalWait, f.arrivalSlack)
		}
	}
}

func TestConfiguredArrivalSlackKeepsYoungAnnouncements(t *testing.T) {
	testTransactionFetcher(t, txFetcherTest{
		init: func() *TxFetcher {
			return NewTxFetcherWithArrivalWait(func(common.Hash) bool { return false }, nil, func(string, uint64, []common.Hash) error { return nil }, nil, 100*time.Millisecond)
		},
		steps: []interface{}{
			doTxNotify{peer: "A", hashes: []common.Hash{{1}}, types: []byte{types.LegacyTxType}, sizes: []uint32{111}},
			doWait{time: 50 * time.Millisecond},
			doTxNotify{peer: "B", hashes: []common.Hash{{2}}, types: []byte{types.LegacyTxType}, sizes: []uint32{222}},
			doWait{time: 50 * time.Millisecond, step: true},
			isWaiting(map[string][]announce{"B": {{common.Hash{2}, types.LegacyTxType, 222}}}),
		},
	})
}

func TestConfiguredArrivalRetrievalLimits(t *testing.T) {
	for _, wait := range []time.Duration{100 * time.Millisecond, 500 * time.Millisecond} {
		for _, size := range []uint32{111, 64 * 1024} {
			t.Run(fmt.Sprintf("%s/%d", wait, size), func(t *testing.T) {
				var f *TxFetcher
				hashes := make([]common.Hash, maxTxRetrievals+1)
				kinds := make([]byte, len(hashes))
				sizes := make([]uint32, len(hashes))
				for i := range hashes {
					hashes[i] = common.Hash{byte(i / 256), byte(i % 256)}
					sizes[i] = size
				}
				testTransactionFetcher(t, txFetcherTest{
					init: func() *TxFetcher {
						f = NewTxFetcherWithArrivalWait(func(common.Hash) bool { return false }, nil, func(string, uint64, []common.Hash) error { return nil }, nil, wait)
						return f
					},
					steps: []interface{}{
						doTxNotify{peer: "A", hashes: hashes, types: kinds, sizes: sizes},
						doWait{time: wait, step: true},
						doFunc(func() {
							request := f.requests["A"]
							want := min(maxTxRetrievals, maxTxRetrievalSize/int(size))
							if request == nil || len(request.hashes) != want {
								t.Fatalf("request exceeds configured retrieval limit: want %d", want)
							}
							if len(f.requests) != 1 {
								t.Fatal("more than one request in flight for one peer")
							}
						}),
					},
				})
			})
		}
	}
}
