package pgq

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// UpdateBuilder builds SQL UPDATE statements.
type UpdateBuilder struct {
	prefixes   []SQLizer
	table      string
	setClauses []setClause
	whereParts []SQLizer
	orderBys   []string
	suffixes   []SQLizer
}

type setClause struct {
	column string
	value  any
}

func (b UpdateBuilder) SQL() (sqlStr string, args []any, err error) {
	if b.table == "" {
		err = fmt.Errorf("update statements must specify a table")
		return
	}
	if len(b.setClauses) == 0 {
		err = fmt.Errorf("update statements must have at least one Set clause")
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

	sql.WriteString("UPDATE ")
	sql.WriteString(b.table)

	sql.WriteString(" SET ")
	setSQLs := make([]string, len(b.setClauses))
	for i, setClause := range b.setClauses {
		var valSQL string
		if vs, ok := setClause.value.(SQLizer); ok {
			vsql, vargs, err := vs.SQL()
			if err != nil {
				return "", nil, err
			}
			if _, ok := vs.(SelectBuilder); ok {
				valSQL = fmt.Sprintf("(%s)", vsql)
			} else {
				valSQL = vsql
			}
			args = append(args, vargs...)
		} else {
			valSQL = "?"
			args = append(args, setClause.value)
		}
		setSQLs[i] = fmt.Sprintf("%s = %s", setClause.column, valSQL)
	}
	sql.WriteString(strings.Join(setSQLs, ", "))

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

// MustSQL builds the query into a SQL string and bound args.
// It panics if there are any errors.
func (b UpdateBuilder) MustSQL() (string, []any) {
	sql, args, err := b.SQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b UpdateBuilder) Prefix(sql string, args ...any) UpdateBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b UpdateBuilder) PrefixExpr(expr SQLizer) UpdateBuilder {
	b.prefixes = append(b.prefixes, expr)
	return b
}

// Table sets the table to be updated.
func (b UpdateBuilder) Table(table string) UpdateBuilder {
	b.table = table
	return b
}

// Set adds SET clauses to the query.
func (b UpdateBuilder) Set(column string, value any) UpdateBuilder {
	b.setClauses = append(b.setClauses, setClause{column: column, value: value})
	return b
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b UpdateBuilder) SetMap(clauses map[string]any) UpdateBuilder {
	keys := make([]string, len(clauses))
	i := 0
	for key := range clauses {
		keys[i] = key
		i++
	}
	sort.Strings(keys)
	for _, key := range keys {
		if val, ok := clauses[key]; ok {
			b = b.Set(key, val)
		}
	}
	return b
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b UpdateBuilder) Where(pred any, args ...any) UpdateBuilder {
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b UpdateBuilder) OrderBy(orderBys ...string) UpdateBuilder {
	b.orderBys = append(b.orderBys, orderBys...)
	return b
}

// Suffix adds an expression to the end of the query
func (b UpdateBuilder) Suffix(sql string, args ...any) UpdateBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b UpdateBuilder) SuffixExpr(expr SQLizer) UpdateBuilder {
	b.suffixes = append(b.suffixes, expr)
	return b
}
