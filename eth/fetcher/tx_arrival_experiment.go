package fetcher

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"os"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/log"
)

// The operator may enable one bounded trial. Expiry restores the configured wait.
// No transaction contents, addresses, or peer identities enter the trace.
type arrivalExperiment struct {
	start, end time.Time
	seed       [32]byte
	events     chan arrivalEvent
	disabled   atomic.Bool
}

type arrivalEvent struct {
	Hash   common.Hash `json:"hash"`
	Stage  string      `json:"stage"`
	UnixNS int64       `json:"unixNs"`
	MonoNS int64       `json:"monoNs"`
	Direct bool        `json:"direct"`
	Result string      `json:"result,omitempty"`
	WaitMS int64       `json:"waitMs,omitempty"`
}

type arrivalExperimentConfig struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Path  string    `json:"path"`
}

func newArrivalExperiment() *arrivalExperiment {
	value := os.Getenv("BOR_DELIVERY_EXPERIMENT")
	if value == "" {
		return nil
	}
	var config arrivalExperimentConfig
	if err := json.Unmarshal([]byte(value), &config); err != nil {
		log.Error("Invalid delivery experiment configuration", "err", err)
		return nil
	}
	now := time.Now()
	if !config.End.After(now) || config.End.Sub(config.Start) > 30*time.Minute || !config.End.After(config.Start) || config.Start.Sub(now) > 30*time.Minute || config.Path == "" {
		log.Error("Delivery experiment outside allowed interval")
		return nil
	}
	file, err := os.OpenFile(config.Path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		log.Error("Delivery trace unavailable", "err", err)
		return nil
	}
	experiment := &arrivalExperiment{start: config.Start, end: config.End, events: make(chan arrivalEvent, 8192)}
	if _, err := rand.Read(experiment.seed[:]); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			log.Error("Close delivery trace", "err", closeErr)
		}
		log.Error("Delivery assignment seed unavailable", "err", err)
		return nil
	}
	go experiment.write(file)
	log.Info("Delivery experiment enabled", "start", config.Start, "end", config.End)
	return experiment
}

func (e *arrivalExperiment) assign(hash common.Hash) time.Duration {
	if e == nil || e.disabled.Load() {
		return 0
	}
	now := time.Now()
	if now.Before(e.start) || !now.Before(e.end) {
		return 0
	}
	input := [64]byte{}
	copy(input[:32], e.seed[:])
	copy(input[32:], hash[:])
	digest := sha256.Sum256(input[:])
	wait := 100 * time.Millisecond
	if digest[0]&1 != 0 {
		wait = 500 * time.Millisecond
	}
	e.record(hash, "enroll", false, "", wait.Milliseconds())
	return wait
}

func (e *arrivalExperiment) record(hash common.Hash, stage string, direct bool, result string, waitMS int64) {
	if e == nil || e.disabled.Load() {
		return
	}
	event := arrivalEvent{hash, stage, time.Now().UnixNano(), int64(mclock.Now()), direct, result, waitMS}
	e.emit(event)
}

func (e *arrivalExperiment) emit(event arrivalEvent) {
	if e == nil || e.disabled.Load() {
		return
	}
	select {
	case e.events <- event:
	default:
		if e.disabled.CompareAndSwap(false, true) {
			log.Error("Delivery trace queue full, new enrollment disabled")
		}
	}
}

// Only this goroutine owns the file. It exits at expiry plus a fixed drain interval.
// Producers never wait for file IO. An incomplete trace invalidates the trial.
func (e *arrivalExperiment) write(file *os.File) {
	defer func() {
		e.disabled.Store(true)
		if err := file.Close(); err != nil {
			log.Error("Close delivery trace", "err", err)
		}
	}()
	writer := bufio.NewWriterSize(file, 65536)
	defer func() {
		if err := writer.Flush(); err != nil {
			log.Error("Flush delivery trace", "err", err)
		}
	}()
	timer := time.NewTimer(time.Until(e.end.Add(15 * time.Second)))
	defer timer.Stop()
	flush := time.NewTicker(time.Second)
	defer flush.Stop()
	bytes := 0
	for {
		select {
		case event := <-e.events:
			data, err := json.Marshal(event)
			if err != nil {
				log.Error("Encode delivery trace", "err", err)
				return
			}
			bytes += len(data) + 1
			if bytes > 256*1024*1024 {
				log.Error("Delivery trace size limit reached")
				return
			}
			if _, err := writer.Write(append(data, '\n')); err != nil {
				log.Error("Write delivery trace", "err", err)
				return
			}
		case <-flush.C:
			if err := writer.Flush(); err != nil {
				log.Error("Flush delivery trace", "err", err)
				return
			}
		case <-timer.C:
			log.Info("Delivery experiment trace complete", "bytes", bytes, "incomplete", e.disabled.Load())
			return
		}
	}
}
