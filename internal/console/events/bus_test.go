// bus_test.go exercises the in-process pub/sub bus that powers live
// row patching for ephemeral UI signals.
//
// Methodology:
//   - Unit tests only — the bus has no external dependencies.
//   - Each test creates its own Bus; nothing is shared.
//   - Bounded waits with t.Deadline-aware contexts so a wedged channel
//     fails the test rather than hanging the suite.
//   - Minimum 2 assertions per test (e.g. one happy-path, one
//     boundary/negative).
package events

import (
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// silentLogger gives tests a /dev/null logger so drop warnings don't
// litter the suite output.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestBus_publishDeliversToMatchingSubscriber(t *testing.T) {
	b := NewBus(silentLogger())
	defer b.Close()
	ch, cancel := b.Subscribe(TopicDLQ)
	defer cancel()
	if b.SubscriberCount() != 1 {
		t.Fatalf("subscriber count = %d, want 1", b.SubscriberCount())
	}
	delivered := b.Publish(Event{
		Topic: TopicDLQ, Op: OpRowRemove, Key: "42",
	})
	if delivered != 1 {
		t.Fatalf("Publish reported %d deliveries, want 1", delivered)
	}
	select {
	case evt := <-ch:
		if evt.Key != "42" {
			t.Fatalf("Key = %q, want %q", evt.Key, "42")
		}
		if evt.Op != OpRowRemove {
			t.Fatalf("Op = %q, want %q", evt.Op, OpRowRemove)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive published event")
	}
}

func TestBus_publishSkipsNonMatchingTopic(t *testing.T) {
	b := NewBus(silentLogger())
	defer b.Close()
	chDLQ, cancelDLQ := b.Subscribe(TopicDLQ)
	defer cancelDLQ()
	chTrig, cancelTrig := b.Subscribe(TopicTrigger)
	defer cancelTrig()
	delivered := b.Publish(Event{
		Topic: TopicTrigger, Op: OpRowReplace, Key: "cron-1",
	})
	if delivered != 1 {
		t.Fatalf("delivered = %d, want 1 (trigger only)", delivered)
	}
	select {
	case <-chDLQ:
		t.Fatal("DLQ subscriber should not receive trigger events")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case evt := <-chTrig:
		if evt.Topic != TopicTrigger {
			t.Fatalf("topic = %q", evt.Topic)
		}
	case <-time.After(time.Second):
		t.Fatal("trigger subscriber did not receive event")
	}
}

func TestBus_cancelUnsubscribesAndClosesChannel(t *testing.T) {
	b := NewBus(silentLogger())
	defer b.Close()
	ch, cancel := b.Subscribe(TopicDLQ)
	if b.SubscriberCount() != 1 {
		t.Fatalf("count = %d, want 1", b.SubscriberCount())
	}
	cancel()
	if b.SubscriberCount() != 0 {
		t.Fatalf("count after cancel = %d, want 0",
			b.SubscriberCount())
	}
	// Channel must be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel returned a value, want closed")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after cancel")
	}
	// Cancel is idempotent — no panic on second call.
	cancel()
}

func TestBus_publishWithFullBufferDropsAndContinues(t *testing.T) {
	b := NewBus(silentLogger())
	defer b.Close()
	// Reduce buffer size for the test.
	b.bufSize = 2
	ch, cancel := b.Subscribe(TopicDLQ)
	defer cancel()
	// Fill the buffer.
	for i := 0; i < 2; i++ {
		b.Publish(Event{Topic: TopicDLQ, Op: OpRowAdd, Key: "k"})
	}
	// Next publish should drop, not block.
	done := make(chan struct{})
	go func() {
		b.Publish(Event{Topic: TopicDLQ, Op: OpRowAdd, Key: "overflow"})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Publish blocked on full buffer; want drop-and-continue")
	}
	// Drain the buffer; both originals must survive, overflow dropped.
	for i := 0; i < 2; i++ {
		select {
		case evt := <-ch:
			if evt.Key == "overflow" {
				t.Fatalf("overflow event leaked through; want dropped")
			}
		case <-time.After(time.Second):
			t.Fatal("could not drain buffer")
		}
	}
}

func TestBus_publishFansOutToManySubscribers(t *testing.T) {
	b := NewBus(silentLogger())
	defer b.Close()
	const subCount = 5
	channels := make([]<-chan Event, 0, subCount)
	cancels := make([]func(), 0, subCount)
	for i := 0; i < subCount; i++ {
		ch, cancel := b.Subscribe(TopicDLQ)
		channels = append(channels, ch)
		cancels = append(cancels, cancel)
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()
	delivered := b.Publish(Event{
		Topic: TopicDLQ, Op: OpRowRemove, Key: "z",
	})
	if delivered != subCount {
		t.Fatalf("delivered = %d, want %d", delivered, subCount)
	}
	var wg sync.WaitGroup
	wg.Add(subCount)
	for _, ch := range channels {
		go func(c <-chan Event) {
			defer wg.Done()
			select {
			case <-c:
			case <-time.After(time.Second):
				t.Errorf("subscriber did not receive event")
			}
		}(ch)
	}
	wg.Wait()
}

func TestBus_closeStopsSubscriptions(t *testing.T) {
	b := NewBus(silentLogger())
	ch, _ := b.Subscribe(TopicDLQ)
	if b.SubscriberCount() != 1 {
		t.Fatalf("count = %d", b.SubscriberCount())
	}
	b.Close()
	// Channel must close.
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel returned value after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after Close()")
	}
	// Subscribe-after-close returns a pre-closed channel.
	post, _ := b.Subscribe(TopicTrigger)
	select {
	case _, ok := <-post:
		if ok {
			t.Fatal("post-close subscribe returned an open channel")
		}
	case <-time.After(time.Second):
		t.Fatal("post-close subscribe channel not closed")
	}
}

func TestBus_publishEmptyTopicPanics(t *testing.T) {
	b := NewBus(silentLogger())
	defer b.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty topic")
		}
	}()
	b.Publish(Event{Op: OpRowAdd})
}

func TestBus_subscribeEmptyTopicPanics(t *testing.T) {
	b := NewBus(silentLogger())
	defer b.Close()
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty subscribe topic")
		}
	}()
	b.Subscribe("")
}
