package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrFull = errors.New("queue full")
var ErrClosed = errors.New("queue closed")

type Item struct {
	Endpoint        string
	Body            []byte
	ContentEncoding string
	Auth            string
	UserAgent       string
	Due             time.Time
	active          bool
}

// Queue leases items until Complete. Count and bytes include in-flight items.
type Queue struct {
	mu       sync.Mutex
	items    []*Item
	bytes    int64
	maxItems int
	maxBytes int64
	policy   string
	blocked  map[string]time.Time
	changed  chan struct{}
	closed   bool
}

func New(maxItems int, maxBytes int64, policy string) *Queue {
	return &Queue{maxItems: maxItems, maxBytes: maxBytes, policy: policy, blocked: make(map[string]time.Time), changed: make(chan struct{})}
}
func (q *Queue) signal()    { close(q.changed); q.changed = make(chan struct{}) }
func (q *Queue) Depth() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.items) }

// Enqueue returns the number of waiting items discarded to make room.
func (q *Queue) Enqueue(it *Item) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return 0, ErrClosed
	}
	if int64(len(it.Body)) > q.maxBytes {
		return 0, ErrFull
	}
	if until := q.blocked[it.Endpoint]; until.After(it.Due) {
		it.Due = until
	}
	// Check that enough waiting items exist before discarding any. In-flight
	// payloads remain owned by workers and cannot be evicted safely.
	if q.policy == "drop-oldest" {
		count, bytes := len(q.items), q.bytes
		for _, candidate := range q.items {
			if count < q.maxItems && bytes+int64(len(it.Body)) <= q.maxBytes {
				break
			}
			if !candidate.active {
				count--
				bytes -= int64(len(candidate.Body))
			}
		}
		if count >= q.maxItems || bytes+int64(len(it.Body)) > q.maxBytes {
			return 0, ErrFull
		}
	}
	dropped := 0
	for len(q.items) >= q.maxItems || q.bytes+int64(len(it.Body)) > q.maxBytes {
		if q.policy == "reject-new" {
			return 0, ErrFull
		}
		oldest := -1
		for i, candidate := range q.items {
			if !candidate.active {
				oldest = i
				break
			}
		}
		if oldest < 0 {
			return dropped, ErrFull
		}
		q.remove(oldest)
		dropped++
	}
	q.items = append(q.items, it)
	q.bytes += int64(len(it.Body))
	q.signal()
	return dropped, nil
}
func (q *Queue) remove(i int) {
	it := q.items[i]
	q.bytes -= int64(len(it.Body))
	copy(q.items[i:], q.items[i+1:])
	q.items[len(q.items)-1] = nil
	q.items = q.items[:len(q.items)-1]
	q.prune(it.Endpoint)
}
func (q *Queue) prune(endpoint string) {
	for _, it := range q.items {
		if it.Endpoint == endpoint {
			return
		}
	}
	delete(q.blocked, endpoint)
}

func (q *Queue) Acquire(ctx context.Context) (*Item, error) {
	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return nil, ErrClosed
		}
		now := time.Now()
		var next time.Time
		for _, it := range q.items {
			if it.active {
				continue
			}
			if !it.Due.After(now) {
				it.active = true
				q.mu.Unlock()
				return it, nil
			}
			if next.IsZero() || it.Due.Before(next) {
				next = it.Due
			}
		}
		changed := q.changed
		q.mu.Unlock()
		if next.IsZero() {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-changed:
			}
			continue
		}
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

// Complete removes a delivered or permanent-failure item. A nonzero retry time reschedules it.
func (q *Queue) Complete(it *Item, retry time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, candidate := range q.items {
		if candidate == it {
			if retry.IsZero() {
				q.remove(i)
			} else {
				if until := q.blocked[it.Endpoint]; until.After(retry) {
					retry = until
				}
				it.Due = retry
				it.active = false
			}
			q.signal()
			return
		}
	}
}

// DelayEndpoint postpones waiting items and future admissions for one endpoint.
func (q *Queue) DelayEndpoint(endpoint string, until time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if until.After(q.blocked[endpoint]) {
		q.blocked[endpoint] = until
	}
	for _, it := range q.items {
		if it.Endpoint == endpoint && !it.active && until.After(it.Due) {
			it.Due = until
		}
	}
	q.signal()
}
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.signal()
	}
}
