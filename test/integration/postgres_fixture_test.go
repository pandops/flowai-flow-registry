//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pandops/flowai-flow-registry/internal/migrations"
	"github.com/pandops/flowai-flow-registry/internal/store"
)

// One PostgreSQL 16 container serves every integration test of this
// package; each test gets its own database and roles, as the deployment
// prepares them: the service role owns its pre-created schema and has no
// CREATE privilege on the database.

const pgImage = "docker.io/library/postgres:16"

type pgFixture struct {
	binary, container, password string
	port                        int
}

var pg *pgFixture

func TestMain(m *testing.M) {
	f, err := startPostgres()
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres fixture: %v\n", err)
		os.Exit(1)
	}
	pg = f
	code := m.Run()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := exec.CommandContext(ctx, f.binary, "rm", "--force", f.container).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "postgres teardown: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	cancel()
	os.Exit(code)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func startPostgres() (*pgFixture, error) {
	var binary string
	for _, c := range []string{"docker", "podman"} {
		if p, err := exec.LookPath(c); err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = exec.CommandContext(ctx, p, "info").Run()
			cancel()
			if err == nil {
				binary = p
				break
			}
		}
	}
	if binary == "" {
		return nil, errors.New("no usable docker or podman")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	f := &pgFixture{binary: binary, container: "flowai-fr-" + randHex(6), password: randHex(12), port: port}
	if ledger := os.Getenv("FLOWAI_RESOURCE_LEDGER"); ledger != "" {
		rec, _ := json.Marshal(map[string]string{"kind": "containers", "selector": "name=^" + f.container + "$"})
		file, err := os.OpenFile(ledger, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		_, werr := file.Write(append(rec, '\n'))
		if err := errors.Join(werr, file.Close()); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, "run", "--detach", "--rm", "--name", f.container,
		"--label", "flowai.flow-registry.test", "--publish", fmt.Sprintf("127.0.0.1:%d:5432", port),
		"--tmpfs", "/var/lib/postgresql/data:rw", "--env", "POSTGRES_USER=flowai",
		"--env", "POSTGRES_PASSWORD="+f.password, "--env", "POSTGRES_DB=postgres", pgImage).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("run container: %w: %s", err, out)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		db, err := sql.Open("pgx", f.dsn("flowai", f.password, "postgres", ""))
		if err == nil {
			err = db.PingContext(ctx)
			db.Close()
			if err == nil {
				return f, nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	_ = exec.Command(binary, "rm", "--force", f.container).Run()
	return nil, errors.New("postgres not ready within 60s")
}

func (f *pgFixture) dsn(user, password, database, searchPath string) string {
	d := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", user, password, f.port, database)
	if searchPath != "" {
		d += "&search_path=" + searchPath
	}
	return d
}

// pgEnv is one test's database: the flow-registry role and connection, a
// foreign service's schema, and a superuser connection for inspection.
type pgEnv struct {
	name, role, password string
	admin                *sql.DB // superuser on the test database
	svc                  *sql.DB // flow-registry role, search_path flow_registry_schema
}

func newPGEnv(t *testing.T, precreate bool) *pgEnv {
	t.Helper()
	ctx := context.Background()
	root, err := sql.Open("pgx", pg.dsn("flowai", pg.password, "postgres", ""))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	e := &pgEnv{name: "fr_" + randHex(6), role: "flow_registry_" + randHex(4), password: randHex(10)}
	other := "model_registry_" + randHex(4)
	for _, q := range []string{
		"CREATE DATABASE " + e.name,
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", e.role, e.password),
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'x'", other),
		fmt.Sprintf("REVOKE ALL ON DATABASE %s FROM PUBLIC", e.name),
		fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", e.name, e.role),
	} {
		if _, err := root.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() {
		r, _ := sql.Open("pgx", pg.dsn("flowai", pg.password, "postgres", ""))
		defer r.Close()
		_, _ = r.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1", e.name)
		for _, q := range []string{"DROP DATABASE IF EXISTS " + e.name, "DROP ROLE IF EXISTS " + e.role, "DROP ROLE IF EXISTS " + other} {
			if _, err := r.Exec(q); err != nil {
				t.Errorf("cleanup %s: %v", q, err)
			}
		}
	})
	e.admin, err = sql.Open("pgx", pg.dsn("flowai", pg.password, e.name, ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.admin.Close() })
	for _, q := range []string{
		"REVOKE CREATE ON SCHEMA public FROM PUBLIC",
		fmt.Sprintf("CREATE SCHEMA model_registry_schema AUTHORIZATION %s", other),
		"CREATE TABLE model_registry_schema.models (team_id bigint, alias text)",
		fmt.Sprintf("ALTER TABLE model_registry_schema.models OWNER TO %s", other),
	} {
		if _, err := e.admin.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if precreate {
		if _, err := e.admin.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA flow_registry_schema AUTHORIZATION %s", e.role)); err != nil {
			t.Fatal(err)
		}
	}
	e.svc = e.open(t)
	return e
}

func (e *pgEnv) open(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", pg.dsn(e.role, e.password, e.name, "flow_registry_schema"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// migratedStore applies the migrations as the service role and returns the store.
func migratedStore(t *testing.T) (*pgEnv, *store.Postgres) {
	t.Helper()
	e := newPGEnv(t, true)
	if err := migrations.Apply(context.Background(), e.svc, "flow_registry_schema"); err != nil {
		t.Fatalf("migrations.Apply: %v", err)
	}
	return e, store.NewPostgres(e.svc)
}

func flowsIn(names ...string) string { return strings.Join(names, ",") }
