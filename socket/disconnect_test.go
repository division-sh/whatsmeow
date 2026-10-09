// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package socket

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coder/websocket"

	waLog "go.mau.fi/whatsmeow/util/log"
)

func disconnectTestConn(t *testing.T) *websocket.Conn {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		accepted <- conn
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	peer := <-accepted
	t.Cleanup(func() { _ = peer.CloseNow() })
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func disconnectTestSocket(ctx context.Context, conn *websocket.Conn) *FrameSocket {
	fs := NewFrameSocket(waLog.Noop, nil)
	fs.parentCtx = ctx
	fs.cancelCtx, fs.cancel = context.WithCancel(ctx)
	fs.conn.Store(conn)
	return fs
}

func TestFrameSocketDisconnectSuppressionBeforeSelection(t *testing.T) {
	conn := disconnectTestConn(t)
	synctest.Test(t, func(t *testing.T) {
		fs := disconnectTestSocket(context.Background(), conn)
		var calls int
		ns, err := newNoiseSocket(fs, nil, nil, nil, func(context.Context, *NoiseSocket, bool) {
			calls++
		})
		if err != nil {
			t.Fatal(err)
		}
		ns.Stop(false, false)
		fs.Close(0)
		synctest.Wait()
		if calls != 0 || fs.GetOnDisconnect() != nil {
			t.Fatal("suppressed disconnect handler was selected")
		}
	})
}

func TestFrameSocketDisconnectSelectedCallbackSurvivesLateSuppression(t *testing.T) {
	conn := disconnectTestConn(t)
	synctest.Test(t, func(t *testing.T) {
		fs := disconnectTestSocket(context.Background(), conn)
		selected, release := make(chan struct{}), make(chan struct{})
		var calls int
		ns, err := newNoiseSocket(fs, nil, nil, nil, func(context.Context, *NoiseSocket, bool) {
			close(selected)
			<-release
			calls++
		})
		if err != nil {
			t.Fatal(err)
		}
		fs.Close(0)
		<-selected
		ns.Stop(false, false)
		if fs.GetOnDisconnect() != nil {
			t.Error("late suppression did not clear future selections")
		}
		close(release)
		synctest.Wait()
		if calls != 1 {
			t.Fatalf("selected callback count = %d, want 1", calls)
		}
	})
}

func TestFrameSocketDisconnectEnabledRemoteClose(t *testing.T) {
	conn := disconnectTestConn(t)
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		fs := disconnectTestSocket(ctx, conn)
		var calls int
		ns, err := newNoiseSocket(fs, nil, nil, nil, func(callbackCtx context.Context, socket *NoiseSocket, remote bool) {
			calls++
			if callbackCtx != ctx || socket == nil || socket.fs != fs || !remote {
				t.Error("remote disconnect changed its callback inputs")
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		fs.Close(0)
		synctest.Wait()
		ns.Stop(false, true)
		if calls != 1 || fs.IsConnected() {
			t.Fatalf("remote close: callback count = %d, connected = %t", calls, fs.IsConnected())
		}
	})
}

func TestFrameSocketDisconnectAtMostOnceClose(t *testing.T) {
	conn := disconnectTestConn(t)
	synctest.Test(t, func(t *testing.T) {
		fs := disconnectTestSocket(context.Background(), conn)
		var calls atomic.Int32
		ns, err := newNoiseSocket(fs, nil, nil, nil, func(context.Context, *NoiseSocket, bool) {
			calls.Add(1)
		})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var closers sync.WaitGroup
		for i := 0; i < 16; i++ {
			closers.Add(1)
			go func() {
				defer closers.Done()
				<-start
				fs.Close(0)
			}()
		}
		close(start)
		closers.Wait()
		ns.Stop(false, true)
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatalf("callback count = %d, want 1", calls.Load())
		}
	})
}

func TestFrameSocketDisconnectReentrantStop(t *testing.T) {
	conn := disconnectTestConn(t)
	synctest.Test(t, func(t *testing.T) {
		fs := disconnectTestSocket(context.Background(), conn)
		var calls int
		ns, err := newNoiseSocket(fs, nil, nil, nil, func(_ context.Context, socket *NoiseSocket, _ bool) {
			socket.Stop(true, false)
			calls++
		})
		if err != nil {
			t.Fatal(err)
		}
		fs.Close(0)
		synctest.Wait()
		if calls != 1 || !ns.destroyed.Load() || fs.GetOnDisconnect() != nil {
			t.Fatal("disconnect callback could not stop its own socket")
		}
	})
}

func TestFrameSocketDisconnectConcurrentInitializeClearClose(t *testing.T) {
	for iteration := 0; iteration < 16; iteration++ {
		t.Run(fmt.Sprintf("%02d", iteration), func(t *testing.T) {
			conn := disconnectTestConn(t)
			synctest.Test(t, func(t *testing.T) {
				fs := disconnectTestSocket(context.Background(), conn)
				var calls atomic.Int32
				var ns *NoiseSocket
				start := make(chan struct{})
				var workers sync.WaitGroup
				for _, work := range []func(){
					func() {
						ns, _ = newNoiseSocket(fs, nil, nil, nil, func(context.Context, *NoiseSocket, bool) {
							calls.Add(1)
						})
					},
					func() {
						for i := 0; i < 32; i++ {
							fs.SetOnDisconnect(nil)
							_ = fs.GetOnDisconnect()
						}
					},
					func() { fs.Close(0) },
				} {
					workers.Add(1)
					go func() {
						defer workers.Done()
						<-start
						work()
					}()
				}
				close(start)
				workers.Wait()
				ns.Stop(false, false)
				synctest.Wait()
				if calls.Load() > 1 {
					t.Fatal("one connection dispatched multiple disconnect callbacks")
				}
			})
		})
	}
}

func TestFrameSocketDisconnectRemoteReadPump(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"web.whatsapp.com"}})
		if err != nil {
			t.Errorf("accept websocket: %v", err)
			return
		}
		_ = conn.CloseNow()
	}))
	t.Cleanup(server.Close)
	fs := NewFrameSocket(waLog.Noop, nil)
	t.Cleanup(func() { fs.Close(0) })
	fs.URL = "ws" + strings.TrimPrefix(server.URL, "http")
	called := make(chan bool, 1)
	fs.SetOnDisconnect(func(ctx context.Context, remote bool) {
		if ctx != t.Context() {
			t.Error("read pump changed callback context")
		}
		called <- remote
	})
	if err := fs.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case remote := <-called:
		if !remote || fs.IsConnected() {
			t.Fatal("read pump did not notify remote closure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("read pump did not dispatch disconnect handler")
	}
}
