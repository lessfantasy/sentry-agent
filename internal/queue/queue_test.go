package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestOverflow(t *testing.T) {
	for _, policy := range []string{"drop-oldest", "reject-new"} {
		t.Run(policy, func(t *testing.T) {
			q := New(2, 10, policy)
			a := &Item{Body: []byte("a")}
			b := &Item{Body: []byte("b")}
			c := &Item{Body: []byte("c")}
			_, _ = q.Enqueue(a)
			_, _ = q.Enqueue(b)
			dropped, err := q.Enqueue(c)
			if policy == "reject-new" {
				if !errors.Is(err, ErrFull) || dropped != 0 {
					t.Fatalf("got %d, %v", dropped, err)
				}
			} else if err != nil || dropped != 1 {
				t.Fatalf("got %d, %v", dropped, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := q.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if policy == "reject-new" && got != a || policy == "drop-oldest" && got != b {
				t.Fatalf("got %p", got)
			}
		})
	}
}

func TestBytesAndInFlight(t *testing.T) {
	q := New(2, 3, "drop-oldest")
	a := &Item{Body: []byte("abc")}
	_, _ = q.Enqueue(a)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, _ := q.Acquire(ctx)
	if got != a {
		t.Fatal("wrong item")
	}
	if _, err := q.Enqueue(&Item{Body: []byte("x")}); !errors.Is(err, ErrFull) {
		t.Fatalf("expected full: %v", err)
	}
	q.Complete(a, time.Time{})
	if _, err := q.Enqueue(&Item{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}
}

func TestDelayedItemDoesNotBlock(t *testing.T) {
	q := New(2, 10, "reject-new")
	a := &Item{Body: []byte("a"), Due: time.Now().Add(time.Hour)}
	b := &Item{Body: []byte("b")}
	_, _ = q.Enqueue(a)
	_, _ = q.Enqueue(b)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := q.Acquire(ctx)
	if err != nil || got != b {
		t.Fatalf("got %p, %v", got, err)
	}
}

func TestConcurrentProducersConsumersAndClose(t *testing.T) {
	q := New(1000, 1000, "reject-new")
	ctx, cancel := context.WithCancel(context.Background())
	var producers, consumers sync.WaitGroup
	for i := 0; i < 4; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for j := 0; j < 100; j++ {
				if _, err := q.Enqueue(&Item{Body: []byte("x")}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for {
				it, err := q.Acquire(ctx)
				if err != nil {
					return
				}
				q.Complete(it, time.Time{})
			}
		}()
	}
	producers.Wait()
	deadline := time.After(time.Second)
	for q.Depth() != 0 {
		select {
		case <-deadline:
			t.Fatal("queue did not empty")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	q.Close()
	consumers.Wait()
}
