package broker

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DialConfig is the broker connection of the service: a tls:// URL, the
// stand CA, the user JWT credentials file and the inbox prefix.
type DialConfig struct {
	Name            string
	URL             string
	CAFile          string
	CredentialsFile string
	InboxPrefix     string
}

// queueGroup spreads requests over the replicas of the service.
const queueGroup = "flow-registry"

// Dial validates the trust configuration and starts a connection that keeps
// retrying in the background; Connected reports whether it is up. It never
// connects anonymously or without TLS.
func Dial(ctx context.Context, c DialConfig) (Conn, error) {
	if !strings.HasPrefix(c.URL, "tls://") {
		return nil, errors.New("broker: url must use tls://")
	}
	pem, err := os.ReadFile(c.CAFile)
	if c.CAFile == "" || err != nil || !x509.NewCertPool().AppendCertsFromPEM(pem) {
		return nil, errors.New("broker: a readable PEM CA file is required")
	}
	creds, err := os.ReadFile(c.CredentialsFile)
	if c.CredentialsFile == "" || err != nil || !strings.Contains(string(creds), "BEGIN NATS USER JWT") || !strings.Contains(string(creds), "BEGIN USER NKEY SEED") {
		return nil, errors.New("broker: a user credentials file with JWT and seed is required")
	}
	if !strings.HasPrefix(c.InboxPrefix, "_INBOX.") {
		return nil, errors.New("broker: inbox prefix must start with _INBOX")
	}
	nc, err := nats.Connect(c.URL,
		nats.Name(c.Name),
		nats.RootCAs(c.CAFile),
		nats.UserCredentials(c.CredentialsFile),
		nats.CustomInboxPrefix(c.InboxPrefix),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("broker: connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("broker: jetstream: %w", err)
	}
	return &natsConn{nc: nc, js: js}, nil
}

type natsConn struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	mu   sync.Mutex
	subs []*nats.Subscription
}

func toHeader(h map[string]string) nats.Header {
	out := nats.Header{}
	for k, v := range h {
		out[k] = []string{v}
	}
	return out
}

func fromHeader(h nats.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
}

func (c *natsConn) Connected() bool { return c.nc.IsConnected() }

func (c *natsConn) Request(ctx context.Context, subject string, header map[string]string, body []byte) (Reply, error) {
	if !c.nc.IsConnected() {
		return Reply{}, ErrDisconnected
	}
	msg := &nats.Msg{Subject: subject, Header: toHeader(header), Data: body}
	r, err := c.nc.RequestMsgWithContext(ctx, msg)
	switch {
	case err == nil:
	case errors.Is(err, nats.ErrConnectionClosed), errors.Is(err, nats.ErrConnectionReconnecting), errors.Is(err, nats.ErrConnectionDraining):
		return Reply{}, ErrDisconnected
	default:
		return Reply{}, ErrNoResponse
	}
	h := fromHeader(r.Header)
	status, _ := strconv.Atoi(h[HeaderStatus])
	return Reply{Status: status, Header: h, Body: r.Data}, nil
}

func (c *natsConn) Publish(ctx context.Context, subject string, header map[string]string, data []byte) error {
	if !c.nc.IsConnected() {
		return ErrDisconnected
	}
	_, err := c.js.PublishMsg(ctx, &nats.Msg{Subject: subject, Header: toHeader(header), Data: data})
	return err
}

// Serve subscribes the handler in the service's queue group; each request
// is answered on its own goroutine.
func (c *natsConn) Serve(subjects []string, h Handler) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range subjects {
		sub, err := c.nc.QueueSubscribe(s, queueGroup, func(m *nats.Msg) {
			go func() {
				r := h(context.Background(), Msg{Subject: m.Subject, Header: fromHeader(m.Header), Data: m.Data})
				header := toHeader(r.Header)
				header.Set(HeaderStatus, strconv.Itoa(r.Status))
				_ = m.RespondMsg(&nats.Msg{Header: header, Data: r.Body})
			}()
		})
		if err != nil {
			return fmt.Errorf("broker: subscribe %s: %w", s, err)
		}
		c.subs = append(c.subs, sub)
	}
	return nil
}

func (c *natsConn) Close() {
	c.mu.Lock()
	for _, s := range c.subs {
		_ = s.Unsubscribe()
	}
	c.subs = nil
	c.mu.Unlock()
	c.nc.Close()
}
