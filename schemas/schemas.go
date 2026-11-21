// Package schemas embeds flow-registry's published JSON Schemas so the
// service validates every request against the same files its callers read.
package schemas

import "embed"

// FS holds every *.json file of this directory.
//
//go:embed *.json
var FS embed.FS
