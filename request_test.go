// Copyright (c) 2026 Youssef Lemew
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"sync"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
)

type responseHandoffContext struct {
	context.Context
	selected chan struct{}
	resume   chan struct{}
	once     sync.Once
}

func (ctx *responseHandoffContext) Done() <-chan struct{} {
	close(ctx.selected)
	<-ctx.resume
	return ctx.Context.Done()
}

func (ctx *responseHandoffContext) release() {
	ctx.once.Do(func() { close(ctx.resume) })
}

type responseHandoffResult struct {
	handled bool
	panic   any
}

func newResponseTestClient() *Client {
	return &Client{responseWaiters: make(map[string]chan<- *waBinary.Node)}
}

func responseTestNode(id string) *waBinary.Node {
	return &waBinary.Node{Tag: "iq", Attrs: waBinary.Attrs{"id": id, "type": "result"}}
}

// Done blocks after receiveResponse has selected and removed its waiter, but
// before its select can deliver. This deterministically exposes a late close.
func beginResponseHandoff(t *testing.T, client *Client, node *waBinary.Node) (*responseHandoffContext, <-chan responseHandoffResult) {
	t.Helper()
	ctx := &responseHandoffContext{
		Context:  context.Background(),
		selected: make(chan struct{}),
		resume:   make(chan struct{}),
	}
	t.Cleanup(ctx.release)
	result := make(chan responseHandoffResult, 1)
	go func() {
		outcome := responseHandoffResult{}
		defer func() {
			outcome.panic = recover()
			result <- outcome
		}()
		outcome.handled = client.receiveResponse(ctx, node)
	}()
	select {
	case <-ctx.selected:
	case <-time.After(time.Second):
		t.Fatal("response receiver did not reach the selected-waiter boundary")
	}
	return ctx, result
}

func completeResponseHandoff(t *testing.T, ctx *responseHandoffContext, result <-chan responseHandoffResult) {
	t.Helper()
	ctx.release()
	select {
	case outcome := <-result:
		if outcome.panic != nil {
			t.Fatalf("selected response waiter panicked: %v", outcome.panic)
		}
		if !outcome.handled {
			t.Fatal("selected response was not handled")
		}
	case <-time.After(time.Second):
		t.Fatal("selected response receiver did not finish")
	}
}

func requireResponse(t *testing.T, ch <-chan *waBinary.Node, expected *waBinary.Node) {
	t.Helper()
	select {
	case actual, open := <-ch:
		if !open || actual != expected {
			t.Fatalf("response: open=%t actual=%p expected=%p", open, actual, expected)
		}
	case <-time.After(time.Second):
		t.Fatal("response was not delivered")
	}
}

func requireNoResponsePanic(t *testing.T) {
	t.Helper()
	if failure := recover(); failure != nil {
		t.Fatalf("response owner panicked: %v", failure)
	}
}

func TestResponseCancelAfterReceiverSelection(t *testing.T) {
	client := newResponseTestClient()
	waiter := client.waitResponse("owned-request")
	node := responseTestNode("owned-request")
	ctx, result := beginResponseHandoff(t, client, node)
	client.cancelResponse("owned-request", waiter)
	completeResponseHandoff(t, ctx, result)
	requireResponse(t, waiter, node)
}

func TestResponseWaiterOwnership(t *testing.T) {
	t.Run("cancel-first", func(t *testing.T) {
		defer requireNoResponsePanic(t)
		client := newResponseTestClient()
		waiter := client.waitResponse("request")
		client.cancelResponse("request", waiter)
		if _, open := <-waiter; open {
			t.Fatal("canceled waiter remained open")
		}
		if client.receiveResponse(context.Background(), responseTestNode("request")) {
			t.Fatal("response claimed a canceled waiter")
		}
	})
	t.Run("response-first", func(t *testing.T) {
		defer requireNoResponsePanic(t)
		client := newResponseTestClient()
		waiter := client.waitResponse("request")
		node := responseTestNode("request")
		if !client.receiveResponse(context.Background(), node) {
			t.Fatal("registered response was not handled")
		}
		client.cancelResponse("request", waiter)
		requireResponse(t, waiter, node)
	})
	t.Run("repeated-cancel", func(t *testing.T) {
		defer requireNoResponsePanic(t)
		client := newResponseTestClient()
		waiter := client.waitResponse("request")
		client.cancelResponse("request", waiter)
		client.cancelResponse("request", waiter)
	})
	t.Run("stale-cancel-keeps-replacement", func(t *testing.T) {
		defer requireNoResponsePanic(t)
		client := newResponseTestClient()
		old := client.waitResponse("request")
		replacement := client.waitResponse("request")
		client.cancelResponse("request", old)
		node := responseTestNode("request")
		if !client.receiveResponse(context.Background(), node) {
			t.Fatal("stale cancellation removed the replacement waiter")
		}
		requireResponse(t, replacement, node)
	})
	t.Run("canceled-old-waiter-keeps-replacement", func(t *testing.T) {
		defer requireNoResponsePanic(t)
		client := newResponseTestClient()
		old := client.waitResponse("request")
		client.cancelResponse("request", old)
		replacement := client.waitResponse("request")
		client.cancelResponse("request", old)
		node := responseTestNode("request")
		if !client.receiveResponse(context.Background(), node) {
			t.Fatal("old cancellation removed the replacement waiter")
		}
		requireResponse(t, replacement, node)
	})
	t.Run("disconnect-then-cancel", func(t *testing.T) {
		defer requireNoResponsePanic(t)
		client := newResponseTestClient()
		waiter := client.waitResponse("request")
		client.clearResponseWaiters(xmlStreamEndNode)
		client.cancelResponse("request", waiter)
		requireResponse(t, waiter, xmlStreamEndNode)
		if client.receiveResponse(context.Background(), responseTestNode("request")) {
			t.Fatal("cleared waiter remained registered")
		}
	})
	t.Run("full-waiter-cleared-once", func(t *testing.T) {
		defer requireNoResponsePanic(t)
		client := newResponseTestClient()
		waiter := client.waitResponse("request")
		node := responseTestNode("request")
		waiter <- node
		client.clearResponseWaiters(xmlStreamEndNode)
		client.cancelResponse("request", waiter)
		client.clearResponseWaiters(xmlStreamEndNode)
		requireResponse(t, waiter, node)
		if _, open := <-waiter; open {
			t.Fatal("full waiter was not closed by clear")
		}
	})
}

func TestResponseSelectedWaiterKeepsReplacement(t *testing.T) {
	for _, clear := range []bool{false, true} {
		name := "cancel"
		if clear {
			name = "disconnect-and-cancel"
		}
		t.Run(name, func(t *testing.T) {
			client := newResponseTestClient()
			old := client.waitResponse("request")
			node := responseTestNode("request")
			ctx, result := beginResponseHandoff(t, client, node)
			replacement := client.waitResponse("request")
			if clear {
				client.clearResponseWaiters(xmlStreamEndNode)
			}
			client.cancelResponse("request", old)
			completeResponseHandoff(t, ctx, result)
			requireResponse(t, old, node)
			if clear {
				requireResponse(t, replacement, xmlStreamEndNode)
			} else {
				if !client.receiveResponse(context.Background(), node) {
					t.Fatal("selected old waiter removed the replacement")
				}
				requireResponse(t, replacement, node)
			}
		})
	}
}
