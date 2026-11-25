// Package prompt renders Flow prompt templates: only {{external_id}} is
// interpolated, {{{{ and }}}} are literal double braces, every other brace
// is rejected.
package prompt

import (
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	MaxTemplateBytes = 16384
	MaxRenderedBytes = 32768
)

var ErrInvalid = errors.New("prompt: invalid template")

const placeholder = "{{external_id}}"

// Validate checks a template without rendering it.
func Validate(template string) error {
	_, err := render(template, "", false)
	return err
}

// Render interpolates externalID into the template.
func Render(template, externalID string) (string, error) {
	return render(template, externalID, true)
}

func render(template, externalID string, bound bool) (string, error) {
	if template == "" || len(template) > MaxTemplateBytes || !utf8.ValidString(template) {
		return "", ErrInvalid
	}
	var b strings.Builder
	for i := 0; i < len(template); {
		switch {
		case strings.HasPrefix(template[i:], "{{{{"):
			b.WriteString("{{")
			i += 4
		case strings.HasPrefix(template[i:], "}}}}"):
			b.WriteString("}}")
			i += 4
		case strings.HasPrefix(template[i:], placeholder):
			b.WriteString(externalID)
			i += len(placeholder)
		case template[i] == '{' || template[i] == '}':
			return "", ErrInvalid
		default:
			b.WriteByte(template[i])
			i++
		}
		if bound && b.Len() > MaxRenderedBytes {
			return "", ErrInvalid
		}
	}
	return b.String(), nil
}
