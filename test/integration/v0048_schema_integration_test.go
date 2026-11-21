//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/pandops/flowai-flow-registry/internal/migrations"
)

// Integration: flow-registry writes only flow_registry_schema with its own
// role; another schema is refused (service-platform "Access to a foreign
// schema is denied").

func TestV0048FlowRegistrySchemaIsolation(t *testing.T) {
	ctx := context.Background()
	t.Run("Migrations run on the pre-created schema without database CREATE", func(t *testing.T) {
		e := newPGEnv(t, true)
		if err := migrations.Apply(ctx, e.svc, "flow_registry_schema"); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if err := migrations.Apply(ctx, e.svc, "flow_registry_schema"); err != nil {
			t.Fatalf("second Apply: %v", err)
		}
		rows, err := e.admin.QueryContext(ctx, `SELECT n.nspname, c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE pg_get_userbyid(c.relowner)=$1 AND n.nspname <> 'pg_toast'`, e.role) // TOAST storage of the schema's own tables
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			var ns, rel string
			_ = rows.Scan(&ns, &rel)
			if ns != "flow_registry_schema" {
				t.Fatalf("object %s.%s outside flow_registry_schema", ns, rel)
			}
			n++
		}
		if n == 0 {
			t.Fatalf("migrations created no objects")
		}
	})

	t.Run("Access to a foreign schema is denied", func(t *testing.T) {
		e, st := migratedStore(t)
		if err := st.Ping(ctx); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		for _, q := range []string{
			"SELECT * FROM model_registry_schema.models",
			"INSERT INTO model_registry_schema.models VALUES (3, '3/m')",
			"CREATE TABLE model_registry_schema.stolen (x int)",
			"CREATE TABLE public.stolen (x int)",
		} {
			_, err := e.svc.ExecContext(ctx, q)
			if err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Fatalf("%s: err = %v, want permission denied", q, err)
			}
		}
	})
}
