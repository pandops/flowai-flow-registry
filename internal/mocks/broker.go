package mocks

import (
	"context"
	"strings"
	"sync"

	"github.com/pandops/flowai-flow-registry/internal/broker"
)

// Responder answers a request sent to another service. Returning
// broker.ErrNoResponse simulates a stopped or silent service.
type Responder func(m broker.Msg) (broker.Reply, error)

// SentRequest is one request flow-registry sent to another service.
type SentRequest struct {
	Subject string
	Header  map[string]string
	Body    []byte
}

// Published is one stream message flow-registry published to JetStream.
type Published struct {
	Subject string
	Header  map[string]string
	Data    []byte
}

// Broker is an in-process broker.Conn. Requests to subjects of other
// services go to registered responders; flow-registry's own subjects are
// served by the handler passed to Serve, after the account mapping's
// rewrite of the final token pub to sub.
type Broker struct {
	mu         sync.Mutex
	responders map[string]Responder
	requests   []SentRequest
	published  []Published
	handler    broker.Handler
	subjects   []string
}

func NewBroker() *Broker {
	return &Broker{responders: map[string]Responder{}}
}

// Respond registers the responder of one ".pub" request subject.
func (b *Broker) Respond(subject string, r Responder) {
	b.mu.Lock()
	b.responders[subject] = r
	b.mu.Unlock()
}

// Requests returns every request sent to another service, optionally only
// those on one subject.
func (b *Broker) Requests(subject string) []SentRequest {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []SentRequest
	for _, r := range b.requests {
		if subject == "" || r.Subject == subject {
			out = append(out, r)
		}
	}
	return out
}

// Published returns every acknowledged JetStream message.
func (b *Broker) Published() []Published {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Published(nil), b.published...)
}

// Subjects returns the subjects passed to Serve.
func (b *Broker) Subjects() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.subjects...)
}

func copyHeader(h map[string]string) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}

func (b *Broker) Request(ctx context.Context, subject string, header map[string]string, body []byte) (broker.Reply, error) {
	b.mu.Lock()
	b.requests = append(b.requests, SentRequest{Subject: subject, Header: copyHeader(header), Body: append([]byte(nil), body...)})
	r := b.responders[subject]
	b.mu.Unlock()
	if r == nil {
		return broker.Reply{}, broker.ErrNoResponse
	}
	return r(broker.Msg{Subject: subject, Header: copyHeader(header), Data: append([]byte(nil), body...)})
}

func (b *Broker) Publish(_ context.Context, subject string, header map[string]string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	msg := Published{Subject: subject, Header: copyHeader(header), Data: append([]byte(nil), data...)}
	b.published = append(b.published, msg)
	return nil
}

func (b *Broker) Serve(subjects []string, h broker.Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subjects = append([]string(nil), subjects...)
	b.handler = h
	return nil
}

func (b *Broker) Connected() bool { return true }

func (b *Broker) Close() {}

// Deliver plays the role of the account mapping: a request a caller
// publishes on a ".pub" subject reaches the subscriber of the matching
// ".sub" subject. It returns the reply flow-registry sends.
func (b *Broker) Deliver(ctx context.Context, pubSubject string, header map[string]string, body []byte) broker.Reply {
	b.mu.Lock()
	h := b.handler
	subjects := b.subjects
	b.mu.Unlock()
	sub := strings.TrimSuffix(pubSubject, ".pub") + ".sub"
	if h == nil || !matchesAny(subjects, sub) {
		return broker.Reply{Status: 0}
	}
	return h(ctx, broker.Msg{Subject: sub, Header: copyHeader(header), Data: append([]byte(nil), body...)})
}

func matchesAny(patterns []string, subject string) bool {
	for _, p := range patterns {
		if subjectMatches(p, subject) {
			return true
		}
	}
	return false
}

// subjectMatches implements NATS wildcard matching ("*" one token, ">" rest).
func subjectMatches(pattern, subject string) bool {
	pt := strings.Split(pattern, ".")
	st := strings.Split(subject, ".")
	for i, p := range pt {
		if p == ">" {
			return len(st) > i
		}
		if i >= len(st) {
			return false
		}
		if p != "*" && p != st[i] {
			return false
		}
	}
	return len(pt) == len(st)
}
