// package pgq provides a fluent SQL generator.
//
// See https://github.com/Masterminds/pgq for examples.
package pgq

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// SQLizer is the interface that wraps the SQL method.
//
// SQL() returns a SQL representation of the SQLizer, along with a slice of arguments.
// If the query cannot be created, it returns an error.
type SQLizer interface {
	SQL() (string, []any, error)
}

// rawSQLizer is expected to do what SQLizer does, but without finalizing placeholders.
// This is useful for nested queries.
type rawSQLizer interface {
	unfinalizedSQL() (string, []any, error)
}

// Debug calls SQL on s and shows the approximate SQL to be executed
//
// If SQL returns an error, the result of this method will look like:
// "[SQL error: %s]" or "[DebugSQLizer error: %s]"
//
// IMPORTANT: As its name suggests, this function should only be used for
// debugging. While the string result *might* be valid SQL, this function does
// not try very hard to ensure it. Additionally, executing the output of this
// function with any untrusted user input is certainly insecure.
func Debug(s SQLizer) string {
	sql, args, err := s.SQL()
	if err != nil {
		return fmt.Sprintf("[SQL error: %s]", err)
	}

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
			if i+1 > len(args) {
				return fmt.Sprintf(
					"[DebugSQLizer error: too many placeholders in %#v for %d args]",
					sql, len(args))
			}
			buf.WriteString(sql[:p])
			fmt.Fprintf(buf, "'%v'", args[i])
			// advance our sql string "cursor" beyond the arg we placed
			sql = sql[p+1:]
			i++
		}
	}
	if i < len(args) {
		return fmt.Sprintf(
			"[DebugSQLizer error: not enough placeholders in %#v for %d args]",
			sql, len(args))
	}
	// "append" any remaning sql that won't need interpolating
	buf.WriteString(sql)
	return buf.String()
}

type part struct {
	pred any
	args []any
}

func newPart(pred any, args ...any) SQLizer {
	return &part{pred, args}
}

func (p part) SQL() (sql string, args []any, err error) {
	switch pred := p.pred.(type) {
	case nil:
		// no-op
	case SQLizer:
		sql, args, err = nestedSQL(pred)
	case string:
		sql = pred
		args = p.args
	default:
		err = fmt.Errorf("expected string or SQLizer, not %T", pred)
	}
	return
}

func nestedSQL(s SQLizer) (string, []any, error) {
	if raw, ok := s.(rawSQLizer); ok {
		return raw.unfinalizedSQL()
	} else {
		return s.SQL()
	}
}

func appendSQL(parts []SQLizer, w io.Writer, sep string, args []any) ([]any, error) {
	for i, p := range parts {
		partSQL, partArgs, err := nestedSQL(p)
		if err != nil {
			return nil, err
		} else if len(partSQL) == 0 {
			continue
		}

		if i > 0 {
			_, err := io.WriteString(w, sep)
			if err != nil {
				return nil, err
			}
		}

		_, err = io.WriteString(w, partSQL)
		if err != nil {
			return nil, err
		}
		args = append(args, partArgs...)
	}
	return args, nil
}
