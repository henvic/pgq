package pgq

import (
	"bytes"
	"fmt"
	"strings"
)

// DeleteBuilder builds SQL DELETE statements.
type DeleteBuilder struct {
	prefixes   []SQLizer
	from       string
	usingParts []SQLizer
	whereParts []SQLizer
	orderBys   []string
	returning  []SQLizer
	suffixes   []SQLizer
}

// SQL builds the query into a SQL string and bound args.
func (b DeleteBuilder) SQL() (sqlStr string, args []any, err error) {
	sqlStr, args, err = b.unfinalizedSQL()
	if err != nil {
		return
	}

	sqlStr, err = dollarPlaceholder(sqlStr)
	return
}

func (b DeleteBuilder) unfinalizedSQL() (sqlStr string, args []any, err error) {
	if b.from == "" {
		err = fmt.Errorf("delete statements must specify a From table")
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

	sql.WriteString("DELETE FROM ")
	sql.WriteString(b.from)

	if len(b.usingParts) > 0 {
		sql.WriteString(" USING ")
		args, err = appendSQL(b.usingParts, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if len(b.whereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = appendSQL(b.whereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(b.orderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		sql.WriteString(strings.Join(b.orderBys, ", "))
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

	sqlStr = sql.String()
	return
}

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b DeleteBuilder) MustSQL() (string, []any) {
	sql, args, err := b.SQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b DeleteBuilder) Prefix(sql string, args ...any) DeleteBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b DeleteBuilder) PrefixExpr(expr SQLizer) DeleteBuilder {
	b.prefixes = append(b.prefixes, expr)
	return b
}

// From sets the table to be deleted from.
func (b DeleteBuilder) From(from string) DeleteBuilder {
	b.from = from
	return b
}

// Using adds USING expressions to the query.
//
// A table expression allowing columns from other tables to appear in the WHERE condition.
// This uses the same syntax as the FROM clause of a SELECT statement.
// Do not repeat the target table unless you intend a self-join (in which case, you must use an alias).
func (b DeleteBuilder) Using(items ...string) DeleteBuilder {
	parts := make([]SQLizer, 0, len(items))
	for _, str := range items {
		parts = append(parts, newPart(str))
	}
	b.usingParts = append(b.usingParts, parts...)
	return b
}

// UsingSelect adds USING expressions to the query similar to Using, but takes a Select statement.
func (b DeleteBuilder) UsingSelect(from SelectBuilder, alias string) DeleteBuilder {
	b.usingParts = append(b.usingParts, Alias{Expr: from, As: alias})
	return b
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b DeleteBuilder) Where(pred any, args ...any) DeleteBuilder {
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b DeleteBuilder) OrderBy(orderBys ...string) DeleteBuilder {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Returning adds RETURNING expressions to the query.
func (b DeleteBuilder) Returning(columns ...string) DeleteBuilder {
	parts := make([]SQLizer, 0, len(columns))
	for _, col := range columns {
		parts = append(parts, newPart(col))
	}
	b.returning = append(b.returning, parts...)
	return b
}

// ReturningSelect adds a RETURNING expressions to the query similar to Using, but takes a Select statement.
func (b DeleteBuilder) ReturningSelect(from SelectBuilder, alias string) DeleteBuilder {
	b.returning = append(b.returning, Alias{Expr: from, As: alias})
	return b
}

// Suffix adds an expression to the end of the query
func (b DeleteBuilder) Suffix(sql string, args ...any) DeleteBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b DeleteBuilder) SuffixExpr(expr SQLizer) DeleteBuilder {
	b.suffixes = append(b.suffixes, expr)
	return b
}
