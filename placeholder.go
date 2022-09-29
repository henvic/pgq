package pgq

import (
	"bytes"
	"fmt"
	"strings"
)

// placeholderFormat is the interface that wraps the ReplacePlaceholders method.
//
// ReplacePlaceholders takes a SQL statement and replaces each question mark
// placeholder with a (possibly different) SQL placeholder.
type placeholderFormat interface {
	ReplacePlaceholders(sql string) (string, error)
}

type placeholderDebugger interface {
	debugPlaceholder() string
}

var (
	// question is a PlaceholderFormat instance that leaves placeholders as
	// question marks.
	question questionFormat

	// dollar is a PlaceholderFormat instance that replaces placeholders with
	// dollar-prefixed positional placeholders (e.g. $1, $2, $3).
	dollar dollarFormat
)

type questionFormat struct{}

func (questionFormat) ReplacePlaceholders(sql string) (string, error) {
	return sql, nil
}

func (questionFormat) debugPlaceholder() string {
	return "?"
}

type dollarFormat struct{}

func (dollarFormat) debugPlaceholder() string {
	return "$"
}

// Placeholders returns a string with count ? placeholders joined with commas.
func Placeholders(count int) string {
	if count < 1 {
		return ""
	}

	return strings.Repeat(",?", count)[1:]
}

func (dollarFormat) ReplacePlaceholders(sql string) (string, error) {
	buf := &bytes.Buffer{}
	i := 0
	for {
		p := strings.Index(sql, "?")
		if p == -1 {
			break
		}

		if len(sql[p:]) > 1 && sql[p:p+2] == "??" { // escape ?? => ?
			buf.WriteString(sql[:p])
			buf.WriteString("?")
			if len(sql[p:]) == 1 {
				break
			}
			sql = sql[p+2:]
		} else {
			i++
			buf.WriteString(sql[:p])
			fmt.Fprintf(buf, "$%d", i)
			sql = sql[p+1:]
		}
	}

	buf.WriteString(sql)
	return buf.String(), nil
}
