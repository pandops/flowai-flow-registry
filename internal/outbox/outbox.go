// Package outbox relays committed outbox rows to JetStream in publisher_seq
// order and marks a row sent only after the publish acknowledgement.
package outbox

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/pandops/flowai-flow-registry/internal/broker"
	"github.com/pandops/flowai-flow-registry/internal/store"
)

// Publisher is the publisher name and Nats-Msg-Id prefix.
const Publisher = "flow-registry"

type Relay struct {
	Store    store.Store
	JS       broker.JetStream
	Logger   *slog.Logger
	Interval time.Duration
}

const batch = 100

// Flush publishes every pending row in order and stops at the first failure.
func (r *Relay) Flush(ctx context.Context) (int, error) {
	sent := 0
	for {
		rows, err := r.Store.PendingOutbox(ctx, batch)
		if err != nil {
			return sent, err
		}
		for _, row := range rows {
			if err := r.JS.Publish(ctx, row.Subject, Headers(row), row.Payload); err != nil {
				return sent, err
			}
			if err := r.Store.MarkSent(ctx, row.Seq); err != nil {
				return sent, err
			}
			sent++
		}
		if len(rows) < batch {
			return sent, nil
		}
	}
}

// Run flushes until ctx ends, retrying after failures.
func (r *Relay) Run(ctx context.Context) {
	interval := r.Interval
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := r.Flush(ctx); err != nil && r.Logger != nil && ctx.Err() == nil {
			r.Logger.Warn("outbox publish failed; will retry", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Headers are the NATS headers of one row: team, Nats-Msg-Id and request_id.
func Headers(row store.OutboxRow) map[string]string {
	h := map[string]string{
		broker.HeaderTeam:  row.Team,
		broker.HeaderMsgID: Publisher + "-" + strconv.FormatInt(row.Seq, 10),
	}
	if row.RequestID > 0 {
		h[broker.HeaderRequestID] = strconv.FormatInt(row.RequestID, 10)
	}
	return h
}
