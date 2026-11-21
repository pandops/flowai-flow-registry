//go:build !integration

package test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// testCAFile writes a self-signed CA certificate and returns its path.
func testCAFile(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "flowai test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// testCredsFile writes a NATS .creds file with a syntactically valid layout.
func testCredsFile(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "user.creds")
	content := "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.e30.c2ln\n------END NATS USER JWT------\n\n" +
		"-----BEGIN USER NKEY SEED-----\nSUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU\n------END USER NKEY SEED------\n"
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// productionConfig is a complete configuration in the deployment's key layout.
func productionConfig(t *testing.T, edits ...func(string) string) []byte {
	t.Helper()
	ca, creds := testCAFile(t), testCredsFile(t)
	y := `http:
  listen: ":8080"
db:
  host: postgres
  port: 5432
  user: flow_registry
  password: s3cret
  schema: flow_registry_schema
  sslmode: verify-full
  ca_file: ` + ca + `
nats:
  url: tls://nats:4222
  ca_file: ` + ca + `
  credentials_file: ` + creds + `
  inbox_prefix: _INBOX.flow-registry
  request_timeout: 5s
cursor_keys:
  - id: 1
    secret_hex: "` + strings.Repeat("ab", 32) + `"
active_cursor_key_id: 1
`
	for _, e := range edits {
		y = e(y)
	}
	return []byte(y)
}

func replaceLine(prefix, with string) func(string) string {
	return func(y string) string {
		lines := strings.Split(y, "\n")
		for i, l := range lines {
			if strings.HasPrefix(strings.TrimSpace(l), prefix) {
				if with == "" {
					lines = append(lines[:i], lines[i+1:]...)
				} else {
					indent := l[:len(l)-len(strings.TrimLeft(l, " "))]
					lines[i] = indent + with
				}
				return strings.Join(lines, "\n")
			}
		}
		return y
	}
}

// goFiles lists the service's Go files outside test/, split by _test suffix.
func goFiles(t *testing.T) (prod, tests []string) {
	t.Helper()
	err := filepath.Walk(serviceDir(), func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(p, ".go") {
			return nil
		}
		if strings.HasSuffix(p, "_test.go") || strings.Contains(p, string(filepath.Separator)+"test"+string(filepath.Separator)) {
			tests = append(tests, p)
		} else {
			prod = append(prod, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", serviceDir(), err)
	}
	return prod, tests
}

// foreignSchemas caches compiled schemas of other services by file path.
var foreignSchemas sync.Map

// validateFile validates data against a schema file of another service.
// Every JSON file of that schemas/ directory is registered under its file
// URL and its $id, so relative references resolve offline.
func validateFile(t testing.TB, path string, data []byte) error {
	t.Helper()
	var sch *jsonschema.Schema
	if v, ok := foreignSchemas.Load(path); ok {
		sch = v.(*jsonschema.Schema)
	} else {
		c := jsonschema.NewCompiler()
		c.AssertFormat()
		files, _ := filepath.Glob(filepath.Join(filepath.Dir(path), "*.json"))
		for _, f := range files {
			raw, err := os.ReadFile(f)
			if err != nil {
				return err
			}
			doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(raw)))
			if err != nil {
				continue // consumers.json and other non-schema data
			}
			_ = c.AddResource("file://"+f, doc)
			if m, ok := doc.(map[string]any); ok {
				if id, ok := m["$id"].(string); ok && strings.Contains(id, "://") {
					_ = c.AddResource(id, doc)
				}
			}
		}
		compiled, err := c.Compile("file://" + path)
		if err != nil {
			return err
		}
		foreignSchemas.Store(path, compiled)
		sch = compiled
	}
	doc, err := jsonschema.UnmarshalJSON(strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	return sch.Validate(doc)
}
