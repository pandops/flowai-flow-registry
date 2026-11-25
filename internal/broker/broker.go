// Package broker is flow-registry's boundary to NATS: core request/reply for
// the operations it serves and calls, and JetStream publishing for its outbox
// relay. Everything above this package uses only these types, so unit tests
// replace the broker with an in-process fake.
package broker

import (
	"context"
	"errors"
)

// Header names used on requests and replies (agents.md rule 9).
const (
	HeaderTeam         = "team"
	HeaderRequestID    = "request_id"
	HeaderOperatorID   = "operator_id"
	HeaderTeamName     = "team_name"
	HeaderAdminSubject = "admin_subject"
	HeaderIfMatch      = "If-Match"
	HeaderStatus       = "status"
	HeaderETag         = "ETag"
	HeaderMsgID        = "Nats-Msg-Id"

	// TeamAdmin is the team header value of every administrator action.
	TeamAdmin = "admin"
)

// Msg is one received request.
type Msg struct {
	Subject string
	Header  map[string]string
	Data    []byte
}

// Reply is the answer to a request: the HTTP-equivalent status travels in
// the NATS reply header "status", other reply headers (ETag) beside it.
type Reply struct {
	Status int
	Header map[string]string
	Body   []byte
}

var (
	// ErrNoResponse means no responder answered or the reply did not arrive
	// within the request timeout; callers return 503 service_unavailable or
	// the more specific code their capability names.
	ErrNoResponse = errors.New("broker: no responder or no reply within the timeout")
	// ErrDisconnected means this service's own broker connection is down;
	// callers return 503 broker_unavailable.
	ErrDisconnected = errors.New("broker: connection unavailable")
)

// Requester sends core NATS requests to other services.
type Requester interface {
	Request(ctx context.Context, subject string, header map[string]string, body []byte) (Reply, error)
}

// JetStream publishes one stream message and waits for its acknowledgement.
type JetStream interface {
	Publish(ctx context.Context, subject string, header map[string]string, data []byte) error
}

// Handler answers one received request.
type Handler func(ctx context.Context, m Msg) Reply

// Conn is the complete broker connection used by the running service.
type Conn interface {
	Requester
	JetStream
	// Serve subscribes the handler to every subject (queue group per service).
	Serve(subjects []string, h Handler) error
	Connected() bool
	Close()
}
