package pgq

import (
	"bytes"
	"errors"
)

// sqlizerBuffer is a helper that allows to write many SQLizers one by one
// without constant checks for errors that may come from SQLizer
type sqlizerBuffer struct {
	bytes.Buffer
	args []any
	err  error
}

// WriteSQL converts SQLizer to SQL strings and writes it to buffer
func (b *sqlizerBuffer) WriteSQL(item SQLizer) {
	if b.err != nil {
		return
	}

	var str string
	var args []any
	str, args, b.err = nestedSQL(item)

	if b.err != nil {
		return
	}

	b.WriteString(str)
	b.WriteByte(' ')
	b.args = append(b.args, args...)
}

func (b *sqlizerBuffer) SQL() (string, []any, error) {
	return b.String(), b.args, b.err
}

// whenPart is a helper structure to describe SQLs "WHEN ... THEN ..." expression
type whenPart struct {
	when SQLizer
	then SQLizer
}

func newWhenPart(when any, then any) whenPart {
	return whenPart{newPart(when), newPart(then)}
}

// CaseBuilder builds SQL CASE construct which could be used as parts of queries.
type CaseBuilder struct {
	whatParts SQLizer
	whenParts []whenPart
	elseParts SQLizer
}

// SQL builds the query into a SQL string and bound args.
func (b CaseBuilder) SQL() (sqlStr string, args []any, err error) {
	if len(b.whenParts) == 0 {
		err = errors.New("case expression must contain at lease one WHEN clause")

		return
	}

	sql := sqlizerBuffer{}

	sql.WriteString("CASE ")
	if b.whatParts != nil {
		sql.WriteSQL(b.whatParts)
	}

	for _, p := range b.whenParts {
		sql.WriteString("WHEN ")
		sql.WriteSQL(p.when)
		sql.WriteString("THEN ")
		sql.WriteSQL(p.then)
	}

	if b.elseParts != nil {
		sql.WriteString("ELSE ")
		sql.WriteSQL(b.elseParts)
	}

	sql.WriteString("END")

	return sql.SQL()
}

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b CaseBuilder) MustSQL() (string, []any) {
	sql, args, err := b.SQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// what sets optional value for CASE construct "CASE [value] ..."
func (b CaseBuilder) what(expr any) CaseBuilder {
	b.whatParts = newPart(expr)
	return b
}

// When adds "WHEN ... THEN ..." part to CASE construct
func (b CaseBuilder) When(when any, then any) CaseBuilder {
	// TODO: performance hint: replace slice of WhenPart with just slice of parts
	// where even indices of the slice belong to "when"s and odd indices belong to "then"s
	b.whenParts = append(b.whenParts, newWhenPart(when, then))
	return b
}

// What sets optional "ELSE ..." part for CASE construct
func (b CaseBuilder) Else(expr any) CaseBuilder {
	b.elseParts = newPart(expr)
	return b
}
