package pgq

import (
	"bytes"
	"strconv"
	"strings"
)

// Placeholders returns a string with count ? placeholders joined with commas.
func Placeholders(count int) string {
	if count < 1 {
		return ""
	}

	return strings.Repeat(",?", count)[1:]
}

// dollarPlaceholder replaces question marks ("?") placeholders with
// dollar-prefixed positional placeholders (e.g. $1, $2, $3).
func dollarPlaceholder(sql string) (string, error) {
	buf := &bytes.Buffer{}
	i := 0
	var itob [20]byte
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
			buf.WriteByte('$')
			buf.Write(strconv.AppendInt(itob[:0], int64(i), 10))
			sql = sql[p+1:]
		}
	}

	buf.WriteString(sql)
	return buf.String(), nil
}
