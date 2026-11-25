//go:build !integration

package test

import (
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// Requirement: service-platform service runs from its own directory and image.
//
// Not applicable to flow-registry: "E2E uses the deployed services" (the E2E
// suite owns it) and "Change to one service selects only its tests"
// (qa-tooling's runner owns it).

func TestV0048ServicePlatformServiceRunsFromItsOwnDirectory(t *testing.T) {
	t.Run("Service imports no other service", func(t *testing.T) {
		prod, tests := goFiles(t)
		if len(prod) == 0 || len(tests) == 0 {
			t.Fatalf("no Go files found under %s", serviceDir())
		}
		const self = "github.com/pandops/flowai-flow-registry/"
		for _, f := range append(prod, tests...) {
			file, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			for _, imp := range file.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				monorepo := path == "github.com/flowai/platform" || strings.HasPrefix(path, "github.com/flowai/platform/")
				otherService := strings.HasPrefix(path, "github.com/pandops/flowai-") && !strings.HasPrefix(path, self)
				if monorepo || otherService {
					t.Fatalf("%s imports another service: %s", f, path)
				}
			}
		}
	})
}
