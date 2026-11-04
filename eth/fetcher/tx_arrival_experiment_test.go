package fetcher

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
)

func TestArrivalWaitExperimentStableAndExpires(t *testing.T) {
	e := &arrivalExperiment{start: time.Now().Add(-time.Minute), end: time.Now().Add(time.Minute), events: make(chan arrivalEvent, 4096)}
	counts := map[time.Duration]int{}
	for i := 0; i < 1000; i++ {
		hash := common.BigToHash(common.Big1)
		hash[0], hash[1] = byte(i), byte(i>>8)
		wait := e.assign(hash)
		if wait != e.assign(hash) {
			t.Fatal("assignment changed")
		}
		counts[wait]++
	}
	for _, wait := range []time.Duration{100 * time.Millisecond, 500 * time.Millisecond} {
		if counts[wait] < 400 || counts[wait] > 600 {
			t.Fatalf("unbalanced policy: %v", counts)
		}
	}
	e.end = time.Now().Add(-time.Second)
	if e.assign(common.Hash{}) != 0 {
		t.Fatal("enrolled after expiry")
	}
}

func TestArrivalWaitExperimentOverflowDisablesEnrollment(t *testing.T) {
	e := &arrivalExperiment{start: time.Now().Add(-time.Minute), end: time.Now().Add(time.Minute), events: make(chan arrivalEvent, 1)}
	e.record(common.Hash{}, "body", false, "", 0)
	e.record(common.Hash{}, "body", false, "", 0)
	if !e.disabled.Load() || e.assign(common.Hash{}) != 0 {
		t.Fatal("overflow did not stop enrollment")
	}
}

func TestArrivalWaitExperimentEarlierDeadline(t *testing.T) {
	clock := new(mclock.Simulated)
	f := NewTxFetcherForTests(nil, nil, nil, nil, clock, time.Now, nil)
	slow, fast := common.Hash{1}, common.Hash{2}
	f.waittime[slow] = clock.Now()
	f.waitPolicy[slow] = 500 * time.Millisecond
	timer := new(mclock.Timer)
	trigger := make(chan struct{}, 1)
	f.rescheduleWait(timer, trigger)
	if f.waitDeadline != clock.Now().Add(500*time.Millisecond) {
		t.Fatal("control deadline changed")
	}
	clock.Run(10 * time.Millisecond)
	f.waittime[fast] = clock.Now()
	f.waitPolicy[fast] = 100 * time.Millisecond
	f.rescheduleWait(timer, trigger)
	clock.Run(99 * time.Millisecond)
	select {
	case <-trigger:
		t.Fatal("early timer")
	default:
	}
	clock.Run(time.Millisecond)
	select {
	case <-trigger:
	default:
		t.Fatal("fast arrival waited behind control")
	}
	(*timer).Stop()
}

func TestArrivalWaitExperimentMixedScheduling(t *testing.T) {
	e := &arrivalExperiment{start: time.Now().Add(-time.Minute), end: time.Now().Add(time.Minute), events: make(chan arrivalEvent, 4096)}
	var fast, slow common.Hash
	for i := 1; i < 256; i++ {
		hash := common.Hash{byte(i)}
		if e.assign(hash) == 100*time.Millisecond {
			fast = hash
		} else {
			slow = hash
		}
	}
	if fast == (common.Hash{}) || slow == (common.Hash{}) {
		t.Fatal("missing policy")
	}
	calls := make(chan string, 4)
	testTransactionFetcher(t, txFetcherTest{
		init: func() *TxFetcher {
			f := NewTxFetcherWithArrivalWait(func(common.Hash) bool { return false }, nil, func(peer string, _ uint64, _ []common.Hash) error { calls <- peer; return nil }, nil, 100*time.Millisecond)
			f.experiment = e
			return f
		},
		steps: []interface{}{
			doTxNotify{peer: "slow", hashes: []common.Hash{slow}},
			doWait{time: 10 * time.Millisecond},
			doTxNotify{peer: "fast", hashes: []common.Hash{fast}},
			doWait{time: 100 * time.Millisecond, step: true},
			doFunc(func() {
				select {
				case peer := <-calls:
					if peer != "fast" {
						t.Fatal("control requested first")
					}
				case <-time.After(time.Second):
					t.Fatal("fast request missing")
				}
			}),
			doWait{time: 390 * time.Millisecond, step: true},
			doFunc(func() {
				select {
				case peer := <-calls:
					if peer != "slow" {
						t.Fatal("wrong control request")
					}
				case <-time.After(time.Second):
					t.Fatal("control request missing")
				}
			}),
		},
	})
}
