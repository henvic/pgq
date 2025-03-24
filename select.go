package pgq

import (
	"bytes"
	"fmt"
	"strings"
)

// SelectBuilder builds SQL SELECT statements.
type SelectBuilder struct {
	placeholder  placeholder
	prefixes     []SQLizer
	options      []string
	columns      []SQLizer
	from         SQLizer
	joins        []SQLizer
	whereParts   []SQLizer
	groupBys     []string
	havingParts  []SQLizer
	orderByParts []SQLizer
	limit        string
	offset       string
	suffixes     []SQLizer
}

// SQL builds the query into a SQL string and bound args.
func (b SelectBuilder) SQL() (sqlStr string, args []any, err error) {
	sqlStr, args, err = b.unfinalizedSQL()
	if err != nil {
		return
	}

	f := b.placeholder
	if f == nil {
		f = dollarPlaceholder
	}
	sqlStr, err = f(sqlStr)
	return
}

func (b SelectBuilder) unfinalizedSQL() (sqlStr string, args []any, err error) {
	if len(b.columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
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

	sql.WriteString("SELECT ")

	if len(b.options) > 0 {
		sql.WriteString(strings.Join(b.options, " "))
		sql.WriteString(" ")
	}

	if len(b.columns) > 0 {
		args, err = appendSQL(b.columns, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if b.from != nil {
		sql.WriteString(" FROM ")
		args, err = appendSQL([]SQLizer{b.from}, sql, "", args)
		if err != nil {
			return
		}
	}

	if len(b.joins) > 0 {
		sql.WriteString(" ")
		args, err = appendSQL(b.joins, sql, " ", args)
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

	if len(b.groupBys) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(b.groupBys, ", "))
	}

	if len(b.havingParts) > 0 {
		sql.WriteString(" HAVING ")
		args, err = appendSQL(b.havingParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(b.orderByParts) > 0 {
		sql.WriteString(" ORDER BY ")
		args, err = appendSQL(b.orderByParts, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if b.limit != "" {
		sql.WriteString(" LIMIT ")
		sql.WriteString(b.limit)
	}

	if b.offset != "" {
		sql.WriteString(" OFFSET ")
		sql.WriteString(b.offset)
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
func (b SelectBuilder) MustSQL() (string, []any) {
	sql, args, err := b.SQL()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b SelectBuilder) Prefix(sql string, args ...any) SelectBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b SelectBuilder) PrefixExpr(expr SQLizer) SelectBuilder {
	b.prefixes = append(b.prefixes, expr)
	return b
}

// Distinct adds a DISTINCT clause to the query.
func (b SelectBuilder) Distinct() SelectBuilder {
	return b.Options("DISTINCT")
}

// Options adds select option to the query
func (b SelectBuilder) Options(options ...string) SelectBuilder {
	b.options = append(b.options, options...)
	return b
}

// Columns adds result columns to the query.
func (b SelectBuilder) Columns(columns ...string) SelectBuilder {
	parts := make([]SQLizer, 0, len(columns))
	for _, str := range columns {
		parts = append(parts, newPart(str))
	}
	b.columns = append(b.columns, parts...)
	return b
}

// RemoveColumns remove all columns from query.
// Must add a new column with Column or Columns methods, otherwise
// return a error.
func (b SelectBuilder) RemoveColumns() SelectBuilder {
	b.columns = nil
	return b
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the columns string, for example:
//
//	Column("IF(col IN ("+pgq.Placeholders(3)+"), 1, 0) as col", 1, 2, 3)
func (b SelectBuilder) Column(column any, args ...any) SelectBuilder {
	b.columns = append(b.columns, newPart(column, args...))
	return b
}

// From sets the FROM clause of the query.
func (b SelectBuilder) From(from string) SelectBuilder {
	b.from = newPart(from)
	return b
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b SelectBuilder) FromSelect(from SelectBuilder, alias string) SelectBuilder {
	// Prevent misnumbered parameters in nested selects
	// See https://github.com/Masterminds/squirrel/issues/183
	from.placeholder = questionPlaceholder
	b.from = Alias{
		Expr: from,
		As:   alias,
	}
	return b
}

// JoinClause adds a join clause to the query.
func (b SelectBuilder) JoinClause(pred any, args ...any) SelectBuilder {
	b.joins = append(b.joins, newPart(pred, args...))
	return b
}

// Join adds a JOIN clause to the query.
func (b SelectBuilder) Join(join string, rest ...any) SelectBuilder {
	return b.JoinClause("JOIN "+join, rest...)
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (b SelectBuilder) LeftJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (b SelectBuilder) RightJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// InnerJoin adds a INNER JOIN clause to the query.
func (b SelectBuilder) InnerJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("INNER JOIN "+join, rest...)
}

// CrossJoin adds a CROSS JOIN clause to the query.
func (b SelectBuilder) CrossJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("CROSS JOIN "+join, rest...)
}

// Where adds an expression to the WHERE clause of the query.
//
// Expressions are ANDed together in the generated SQL.
//
// Where accepts several types for its pred argument:
//
// nil OR "" - ignored.
//
// string - SQL expression.
// If the expression has SQL placeholders then a set of arguments must be passed
// as well, one for each placeholder.
//
// map[string]any OR Eq - map of SQL expressions to values. Each key is
// transformed into an expression like "<key> = ?", with the corresponding value
// bound to the placeholder. If the value is nil, the expression will be "<key>
// IS NULL". If the value is an array or slice, the expression will be "<key> = ANY
// (?)". These expressions
// are ANDed together.
//
// Where will panic if pred isn't any of the above types.
func (b SelectBuilder) Where(pred any, args ...any) SelectBuilder {
	if pred == nil || pred == "" {
		return b
	}
	b.whereParts = append(b.whereParts, newWherePart(pred, args...))
	return b
}

// GroupBy adds GROUP BY expressions to the query.
func (b SelectBuilder) GroupBy(groupBys ...string) SelectBuilder {
	b.groupBys = append(b.groupBys, groupBys...)
	return b
}

// Having adds an expression to the HAVING clause of the query.
//
// See Where.
func (b SelectBuilder) Having(pred any, rest ...any) SelectBuilder {
	b.havingParts = append(b.havingParts, newWherePart(pred, rest...))
	return b
}

// RemoveOrderBy removes ORDER BY clause.
func (b SelectBuilder) RemoveOrderBy() SelectBuilder {
	b.orderByParts = nil
	return b
}

// OrderByClause adds ORDER BY clause to the query.
func (b SelectBuilder) OrderByClause(pred any, args ...any) SelectBuilder {
	b.orderByParts = append(b.orderByParts, newPart(pred, args...))
	return b
}

// OrderBy adds ORDER BY expressions to the query.
func (b SelectBuilder) OrderBy(orderBys ...string) SelectBuilder {
	for _, orderBy := range orderBys {
		b = b.OrderByClause(orderBy)
	}

	return b
}

// Limit sets a LIMIT clause on the query.
func (b SelectBuilder) Limit(limit uint64) SelectBuilder {
	b.limit = fmt.Sprintf("%d", limit)
	return b
}

// Limit ALL allows to access all records with limit
func (b SelectBuilder) RemoveLimit() SelectBuilder {
	b.limit = ""
	return b
}

// Offset sets a OFFSET clause on the query.
func (b SelectBuilder) Offset(offset uint64) SelectBuilder {
	b.offset = fmt.Sprintf("%d", offset)
	return b
}

// RemoveOffset removes OFFSET clause.
func (b SelectBuilder) RemoveOffset() SelectBuilder {
	b.offset = ""
	return b
}

// Suffix adds an expression to the end of the query
func (b SelectBuilder) Suffix(sql string, args ...any) SelectBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b SelectBuilder) SuffixExpr(expr SQLizer) SelectBuilder {
	b.suffixes = append(b.suffixes, expr)
	return b
}
