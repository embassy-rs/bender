package main

import (
	"testing"
	"time"
)

func TestSeenDeliveries(t *testing.T) {
	d := newSeenDeliveries()

	if d.has("abc") {
		t.Error("expected unknown GUID to not be seen")
	}

	d.add("abc")
	if !d.has("abc") {
		t.Error("expected added GUID to be seen")
	}

	// An empty GUID (a delivery with no X-GitHub-Delivery header) must not be
	// recorded, or it'd swallow every other GUID-less delivery.
	d.add("")
	if d.has("") {
		t.Error("expected empty GUID to be ignored")
	}

	// Pruning keeps entries inside the window and drops the ones outside it.
	d.mu.Lock()
	d.seen["old"] = time.Now().Add(-deliveryMemory - time.Minute)
	d.mu.Unlock()

	d.prune(time.Now())
	if d.has("old") {
		t.Error("expected GUID older than deliveryMemory to be pruned")
	}
	if !d.has("abc") {
		t.Error("expected recent GUID to survive pruning")
	}
}
