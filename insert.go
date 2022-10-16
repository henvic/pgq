package pgq

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder struct {
	prefixes      []SQLizer
	replace       bool
	into          string
	columns       []string
	values        [][]any
	returning     []SQLizer
	suffixes      []SQLizer
	selectBuilder *SelectBuilder
}

// SQL builds the query into a SQL string and bound args.
func (b InsertBuilder) SQL() (sqlStr string, args []any, err error) {
	if b.into == "" {
		err = errors.New("insert statements must specify a table")
		return
	}
	if len(b.values) == 0 && b.selectBuilder == nil {
		err = errors.New("insert statements must have at least one set of values or select clause")
		return
	}

	sql := &bytes.Buffer{}

	if len(b.prefixes) > 0 {
		args, err = appendSQL(b.prefixes, sql, " ", args)
		if err != nil {
			return
		}

		sql.WriteString(" ")
	}

	if !b.replace {
		sql.WriteString("INSERT ")
	} else {
		sql.WriteString("REPLACE ")
	}

	sql.WriteString("INTO ")
	sql.WriteString(b.into)
	sql.WriteString(" ")

	if len(b.columns) > 0 {
		sql.WriteString("(")
		sql.WriteString(strings.Join(b.columns, ","))
		sql.WriteString(") ")
	}

	if b.selectBuilder != nil {
		args, err = b.appendSelectToSQL(sql, args)
	} else {
		args, err = b.appendValuesToSQL(sql, args)
	}
	if err != nil {
		return
	}

	if len(b.returning) > 0 {
		sql.WriteString(" RETURNING ")
		args, err = appendSQL(b.returning, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if len(b.suffixes) > 0 {
		sql.WriteString(" ")
		args, err = appendSQL(b.suffixes, sql, " ", args)
		if err != nil {
			return
		}
	}

	sqlStr, err = dollarPlaceholder(sql.String())
	return
}

func (b InsertBuilder) appendValuesToSQL(w io.Writer, args []any) ([]any, error) {
	if len(b.values) == 0 {
		return args, errors.New("values for insert statements are not set")
	}

	io.WriteString(w, "VALUES ")

	valuesStrings := make([]string, len(b.values))
	for r, row := range b.values {
		valueStrings := make([]string, len(row))
		for v, val := range row {
			if vs, ok := val.(SQLizer); ok {
				vsql, vargs, err := vs.SQL()
				if err != nil {
					return nil, err
				}
				valueStrings[v] = vsql
				args = append(args, vargs...)
			} else {
				valueStrings[v] = "?"
				args = append(args, val)
			}
		}
		valuesStrings[r] = fmt.Sprintf("(%s)", strings.Join(valueStrings, ","))
	}

	io.WriteString(w, strings.Join(valuesStrings, ","))

	return args, nil
}

func (b InsertBuilder) appendSelectToSQL(w io.Writer, args []any) ([]any, error) {
	if b.selectBuilder == nil {
		return args, errors.New("select clause for insert statements are not set")
	}

	selectClause, sArgs, err := b.selectBuilder.SQL()
	if err != nil {
		return args, err
	}

	io.WriteString(w, selectClause)
	args = append(args, sArgs...)

	return args, nil
}

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b InsertBuilder) MustSQL() (string, []any) {
	sql, args, err := b.SQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b InsertBuilder) Prefix(sql string, args ...any) InsertBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b InsertBuilder) PrefixExpr(expr SQLizer) InsertBuilder {
	b.prefixes = append(b.prefixes, expr)
	return b
}

// Into sets the INTO clause of the query.
func (b InsertBuilder) Into(from string) InsertBuilder {
	b.into = from
	return b
}

// Columns adds insert columns to the query.
func (b InsertBuilder) Columns(columns ...string) InsertBuilder {
	b.columns = append(b.columns, columns...)
	return b
}

// Values adds a single row's values to the query.
func (b InsertBuilder) Values(values ...any) InsertBuilder {
	b.values = append(b.values, values)
	return b
}

// Returning adds RETURNING expressions to the query.
func (b InsertBuilder) Returning(columns ...string) InsertBuilder {
	parts := make([]SQLizer, 0, len(columns))
	for _, col := range columns {
		parts = append(parts, newPart(col))
	}
	b.returning = append(b.returning, parts...)
	return b
}

// ReturningSelect adds a RETURNING expressions to the query similar to Using, but takes a Select statement.
func (b InsertBuilder) ReturningSelect(from SelectBuilder, alias string) InsertBuilder {
	b.returning = append(b.returning, Alias{Expr: from, As: alias})
	return b
}

// Suffix adds an expression to the end of the query
func (b InsertBuilder) Suffix(sql string, args ...any) InsertBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b InsertBuilder) SuffixExpr(expr SQLizer) InsertBuilder {
	b.suffixes = append(b.suffixes, expr)
	return b
}

// SetMap set columns and values for insert builder from a map of column name and value
// note that it will reset all previous columns and values was set if any
func (b InsertBuilder) SetMap(clauses map[string]any) InsertBuilder {
	// Keep the columns in a consistent order by sorting the column key string.
	cols := make([]string, 0, len(clauses))
	for col := range clauses {
		cols = append(cols, col)
	}
	sort.Strings(cols)

	vals := make([]any, 0, len(clauses))
	for _, col := range cols {
		vals = append(vals, clauses[col])
	}

	b.columns = cols
	b.values = [][]any{vals}
	return b
}

// Select set Select clause for insert query
// If Values and Select are used, then Select has higher priority
func (b InsertBuilder) Select(sb SelectBuilder) InsertBuilder {
	b.selectBuilder = &sb
	return b
}
