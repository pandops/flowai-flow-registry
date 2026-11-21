//go:build !integration

package test

import "errors"

var errInjected = errors.New("injected commit failure")
