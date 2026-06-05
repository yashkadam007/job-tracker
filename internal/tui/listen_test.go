package tui

import (
	"context"
	"errors"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/jackc/pgx/v5/pgconn"
)

type blockingNotificationConn struct {
	released chan struct{}
}

func (c *blockingNotificationConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (c *blockingNotificationConn) Release() {
	close(c.released)
}

func TestWaitForJobsChangedCancelsBlockedNotificationWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &blockingNotificationConn{released: make(chan struct{})}

	done := make(chan jobsChangedMsg, 1)
	go func() {
		done <- waitForJobsChanged(ctx, conn)
	}()

	cancel()

	select {
	case msg := <-done:
		if !errors.Is(msg.err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", msg.err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("waitForJobsChanged did not return after context cancellation")
	}

	select {
	case <-conn.released:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("connection was not released after context cancellation")
	}
}

func TestQuitCancelsListenContext(t *testing.T) {
	m := New(Config{})

	next, _ := m.handleKey(tea.KeyMsg{
		Type:  tea.KeyRunes,
		Runes: []rune{'q'},
	})
	got := next.(Model)

	select {
	case <-got.listenCtx.Done():
	case <-time.After(250 * time.Millisecond):
		t.Fatal("q did not cancel the LISTEN context")
	}
}
