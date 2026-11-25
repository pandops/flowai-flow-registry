// Package logging builds flow-registry's JSON slog logger.
package logging

import (
	"io"
	"log/slog"
)

// New returns a JSON logger tagged with service=flow-registry.
func New(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, nil)).With("service", "flow-registry")
}
