// Package cursor issues and verifies flow-registry's opaque list cursors:
// HMAC-SHA256 under an integer configured key_id, bound to the operation,
// the verified team and operator, the filters, the order and the direction.
package cursor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/pandops/flowai-flow-registry/internal/store"
)

var ErrInvalid = errors.New("cursor: invalid")

// Key is one configured HMAC key.
type Key struct {
	ID     int64
	Secret []byte
}

// Binding is what a cursor is valid for.
type Binding struct {
	Operation  string
	TeamID     int64
	OperatorID int64
	Filters    map[string]string
}

// Codec encodes and decodes cursors.
type Codec struct {
	keys   map[int64][]byte
	active int64
	ttl    time.Duration
	now    func() time.Time
}

const order = "created_at,flow_id:asc:forward"

// New builds a codec that signs with the active key and verifies with any configured key.
func New(keys []Key, activeID int64, ttl time.Duration, now func() time.Time) (*Codec, error) {
	c := &Codec{keys: map[int64][]byte{}, active: activeID, ttl: ttl, now: now}
	for _, k := range keys {
		if k.ID < 1 || len(k.Secret) < 32 {
			return nil, errors.New("cursor: key needs a positive id and at least 32 bytes")
		}
		c.keys[k.ID] = append([]byte(nil), k.Secret...)
	}
	if _, ok := c.keys[activeID]; !ok {
		return nil, errors.New("cursor: active key is not configured")
	}
	if ttl <= 0 || now == nil {
		return nil, errors.New("cursor: ttl and clock are required")
	}
	return c, nil
}

type payload struct {
	CreatedAt int64 `json:"c"` // UnixNano
	FlowID    int64 `json:"f"`
	Expires   int64 `json:"e"` // Unix seconds
}

func bindingBytes(b Binding) []byte {
	keys := make([]string, 0, len(b.Filters))
	for k := range b.Filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var pairs [][2]string
	for _, k := range keys {
		pairs = append(pairs, [2]string{k, b.Filters[k]})
	}
	data, _ := json.Marshal([]any{b.Operation, b.TeamID, b.OperatorID, pairs, order})
	return data
}

func (c *Codec) mac(keyID int64, body []byte, b Binding) []byte {
	m := hmac.New(sha256.New, c.keys[keyID])
	m.Write(body)
	m.Write(bindingBytes(b))
	return m.Sum(nil)
}

// Encode signs the position for the binding: base64url(key_id(8) | len(2) | payload | mac(32)).
func (c *Codec) Encode(b Binding, pos store.Position) (string, error) {
	body, _ := json.Marshal(payload{CreatedAt: pos.CreatedAt.UnixNano(), FlowID: pos.FlowID, Expires: c.now().Add(c.ttl).Unix()})
	out := make([]byte, 10, 10+len(body)+32)
	binary.BigEndian.PutUint64(out, uint64(c.active))
	binary.BigEndian.PutUint16(out[8:], uint16(len(body)))
	out = append(out, body...)
	out = append(out, c.mac(c.active, out, b)...)
	return base64.RawURLEncoding.EncodeToString(out), nil
}

// Decode verifies the token for the binding and returns its position.
func (c *Codec) Decode(token string, b Binding) (store.Position, error) {
	if len(token) == 0 || len(token) > 1024 {
		return store.Position{}, ErrInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) < 10+32 {
		return store.Position{}, ErrInvalid
	}
	keyID := int64(binary.BigEndian.Uint64(raw))
	n := int(binary.BigEndian.Uint16(raw[8:]))
	if _, ok := c.keys[keyID]; !ok || keyID < 1 || len(raw) != 10+n+32 {
		return store.Position{}, ErrInvalid
	}
	if !hmac.Equal(raw[10+n:], c.mac(keyID, raw[:10+n], b)) {
		return store.Position{}, ErrInvalid
	}
	var p payload
	if err := json.Unmarshal(raw[10:10+n], &p); err != nil || p.FlowID < 1 {
		return store.Position{}, ErrInvalid
	}
	if c.now().Unix() > p.Expires {
		return store.Position{}, ErrInvalid
	}
	return store.Position{CreatedAt: time.Unix(0, p.CreatedAt).UTC(), FlowID: p.FlowID}, nil
}
