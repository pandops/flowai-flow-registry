// Package config loads flow-registry's YAML configuration. It requires the
// complete production trust configuration: NATS over TLS with the stand CA
// and a user JWT credentials file, and PostgreSQL with sslmode verify-full.
package config

import (
	"bytes"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultSchema = "flow_registry_schema"

// DB is the PostgreSQL connection. Name defaults to "flowai", Schema to
// DefaultSchema; SSLMode must be verify-full with a CAFile.
type DB struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
	Schema   string `yaml:"schema"`
	SSLMode  string `yaml:"sslmode"`
	CAFile   string `yaml:"ca_file"`
}

// NATS is the broker connection: a tls:// URL, the stand CA, the user JWT
// credentials file, the inbox prefix and the request timeout (default 5s).
type NATS struct {
	URL             string `yaml:"url"`
	CAFile          string `yaml:"ca_file"`
	CredentialsFile string `yaml:"credentials_file"`
	InboxPrefix     string `yaml:"inbox_prefix"`
	RequestTimeout  string `yaml:"request_timeout"`
}

type HTTP struct {
	Listen string `yaml:"listen"`
}

type CursorKey struct {
	ID         int64  `yaml:"id"`
	SecretHex  string `yaml:"secret_hex"`
	SecretFile string `yaml:"secret_file"`
}

type Config struct {
	DB                DB          `yaml:"db"`
	NATS              NATS        `yaml:"nats"`
	HTTP              HTTP        `yaml:"http"`
	CursorKeys        []CursorKey `yaml:"cursor_keys"`
	ActiveCursorKeyID int64       `yaml:"active_cursor_key_id"`
	CursorTTLSeconds  int         `yaml:"cursor_ttl_seconds"`

	requestTimeout time.Duration
	secrets        map[int64][]byte
}

var schemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Load reads, defaults and validates the YAML file at path.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return Parse(data)
}

// Parse defaults and validates YAML content.
func Parse(data []byte) (Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if c.DB.Name == "" {
		c.DB.Name = "flowai"
	}
	if c.DB.Schema == "" {
		c.DB.Schema = DefaultSchema
	}
	if c.HTTP.Listen == "" {
		c.HTTP.Listen = ":8080"
	}
	if c.NATS.InboxPrefix == "" {
		c.NATS.InboxPrefix = "_INBOX.flow-registry"
	}
	if c.CursorTTLSeconds == 0 {
		c.CursorTTLSeconds = 86400
	}
	c.requestTimeout = 5 * time.Second
	if c.NATS.RequestTimeout != "" {
		d, err := time.ParseDuration(c.NATS.RequestTimeout)
		if err != nil || d <= 0 {
			return Config{}, errors.New("config: nats.request_timeout must be a positive duration")
		}
		c.requestTimeout = d
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c *Config) validate() error {
	var errs []error
	fail := func(format string, a ...any) { errs = append(errs, fmt.Errorf("config: "+format, a...)) }
	if c.DB.Host == "" || c.DB.User == "" || c.DB.Password == "" {
		fail("db.host, db.user and db.password are required")
	}
	if c.DB.Port < 1 || c.DB.Port > 65535 {
		fail("db.port must be 1..65535")
	}
	if !schemaName.MatchString(c.DB.Schema) || strings.HasPrefix(c.DB.Schema, "pg_") || c.DB.Schema == "public" || c.DB.Schema == "information_schema" {
		fail("db.schema %q is not a valid own schema name", c.DB.Schema)
	}
	if c.DB.SSLMode != "verify-full" {
		fail("db.sslmode must be verify-full")
	}
	if err := checkCA(c.DB.CAFile); err != nil {
		fail("db.ca_file: %v", err)
	}
	if !strings.HasPrefix(c.NATS.URL, "tls://") {
		fail("nats.url must use tls://")
	}
	if err := checkCA(c.NATS.CAFile); err != nil {
		fail("nats.ca_file: %v", err)
	}
	if err := checkCreds(c.NATS.CredentialsFile); err != nil {
		fail("nats.credentials_file: %v", err)
	}
	if !strings.HasPrefix(c.NATS.InboxPrefix, "_INBOX.") {
		fail("nats.inbox_prefix must start with _INBOX.")
	}
	if c.CursorTTLSeconds < 60 {
		fail("cursor_ttl_seconds must be at least 60")
	}
	secrets, err := c.loadSecrets()
	if err != nil {
		fail("%v", err)
	}
	c.secrets = secrets
	return errors.Join(errs...)
}

func (c *Config) loadSecrets() (map[int64][]byte, error) {
	if len(c.CursorKeys) == 0 {
		return nil, errors.New("cursor_keys: at least one key is required")
	}
	out := map[int64][]byte{}
	for _, k := range c.CursorKeys {
		if k.ID < 1 {
			return nil, errors.New("cursor_keys: id must be a positive integer")
		}
		if _, dup := out[k.ID]; dup {
			return nil, fmt.Errorf("cursor_keys: duplicate id %d", k.ID)
		}
		hexValue := k.SecretHex
		if k.SecretFile != "" {
			if hexValue != "" {
				return nil, fmt.Errorf("cursor_keys[%d]: set secret_hex or secret_file, not both", k.ID)
			}
			data, err := os.ReadFile(k.SecretFile)
			if err != nil {
				return nil, fmt.Errorf("cursor_keys[%d]: %w", k.ID, err)
			}
			hexValue = strings.TrimSpace(string(data))
		}
		secret, err := hex.DecodeString(hexValue)
		if err != nil || len(secret) < 32 {
			return nil, fmt.Errorf("cursor_keys[%d]: secret must be at least 32 bytes of hex", k.ID)
		}
		out[k.ID] = secret
	}
	if _, ok := out[c.ActiveCursorKeyID]; !ok {
		return nil, errors.New("active_cursor_key_id must name a configured key")
	}
	return out, nil
}

func checkCA(path string) error {
	if path == "" {
		return errors.New("required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !x509.NewCertPool().AppendCertsFromPEM(data) {
		return errors.New("no PEM certificate")
	}
	return nil
}

func checkCreds(path string) error {
	if path == "" {
		return errors.New("required; the service never connects anonymously")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	s := string(data)
	if !strings.Contains(s, "BEGIN NATS USER JWT") || !strings.Contains(s, "BEGIN USER NKEY SEED") {
		return errors.New("must hold a user JWT and its seed")
	}
	return nil
}

// DSN is the pgx connection string with TLS verification and search_path.
func (c Config) DSN() string {
	q := url.Values{}
	q.Set("search_path", c.DB.Schema)
	q.Set("sslmode", c.DB.SSLMode)
	q.Set("sslrootcert", c.DB.CAFile)
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.DB.User, c.DB.Password),
		Host:     c.DB.Host + ":" + strconv.Itoa(c.DB.Port),
		Path:     "/" + c.DB.Name,
		RawQuery: q.Encode(),
	}
	return u.String()
}

// RequestTimeout is the broker request timeout (default 5 s).
func (c Config) RequestTimeout() time.Duration {
	if c.requestTimeout == 0 {
		return 5 * time.Second
	}
	return c.requestTimeout
}

// CursorTTL is the cursor lifetime (default 24 h).
func (c Config) CursorTTL() time.Duration {
	if c.CursorTTLSeconds == 0 {
		return 24 * time.Hour
	}
	return time.Duration(c.CursorTTLSeconds) * time.Second
}

// CursorSecrets returns each configured key's bytes by id.
func (c Config) CursorSecrets() (map[int64][]byte, error) {
	if c.secrets == nil {
		return c.loadSecrets()
	}
	return c.secrets, nil
}
