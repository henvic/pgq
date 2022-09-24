package pgq

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const (
	// Portable true/false literals.
	sqlTrue  = "(1=1)"
	sqlFalse = "(1=0)"
)

type expr struct {
	sql  string
	args []any
}

// Expr builds an expression from a SQL fragment and arguments.
//
// Ex:
//
//	Expr("FROM_UNIXTIME(?)", t)
func Expr(sql string, args ...any) SQLizer {
	return expr{sql: sql, args: args}
}

func (e expr) SQL() (sql string, args []any, err error) {
	simple := true
	for _, arg := range e.args {
		if _, ok := arg.(SQLizer); ok {
			simple = false
		}
	}
	if simple {
		return e.sql, e.args, nil
	}

	buf := &bytes.Buffer{}
	ap := e.args
	sp := e.sql

	var isql string
	var iargs []any

	for err == nil && len(ap) > 0 && len(sp) > 0 {
		i := strings.Index(sp, "?")
		if i < 0 {
			// no more placeholders
			break
		}
		if len(sp) > i+1 && sp[i+1:i+2] == "?" {
			// escaped "??"; append it and step past
			buf.WriteString(sp[:i+2])
			sp = sp[i+2:]
			continue
		}

		if as, ok := ap[0].(SQLizer); ok {
			// sqlizer argument; expand it and append the result
			isql, iargs, err = as.SQL()
			buf.WriteString(sp[:i])
			buf.WriteString(isql)
			args = append(args, iargs...)
		} else {
			// normal argument; append it and the placeholder
			buf.WriteString(sp[:i+1])
			args = append(args, ap[0])
		}

		// step past the argument and placeholder
		ap = ap[1:]
		sp = sp[i+1:]
	}

	// append the remaining sql and arguments
	buf.WriteString(sp)
	return buf.String(), append(args, ap...), err
}

type concatExpr []any

func (ce concatExpr) SQL() (sql string, args []any, err error) {
	for _, part := range ce {
		switch p := part.(type) {
		case string:
			sql += p
		case SQLizer:
			pSql, pArgs, err := p.SQL()
			if err != nil {
				return "", nil, err
			}
			sql += pSql
			args = append(args, pArgs...)
		default:
			return "", nil, fmt.Errorf("%#v is not a string or SQLizer", part)
		}
	}
	return
}

// ConcatExpr builds an expression by concatenating strings and other expressions.
//
// Ex:
//
//	name_expr := Expr("CONCAT(?, ' ', ?)", firstName, lastName)
//	ConcatExpr("COALESCE(full_name,", name_expr, ")")
func ConcatExpr(parts ...any) concatExpr {
	return concatExpr(parts)
}

// aliasExpr helps to alias part of SQL query generated with underlying "expr"
type aliasExpr struct {
	expr  SQLizer
	alias string
}

// Alias allows to define alias for column in SelectBuilder. Useful when column is
// defined as complex expression like IF or CASE
// Ex:
//
//	.Column(Alias(caseStmt, "case_column"))
func Alias(expr SQLizer, alias string) aliasExpr {
	return aliasExpr{expr, alias}
}

func (e aliasExpr) SQL() (sql string, args []any, err error) {
	sql, args, err = e.expr.SQL()
	if err == nil {
		sql = fmt.Sprintf("(%s) AS %s", sql, e.alias)
	}
	return
}

// Eq is syntactic sugar for use with Where/Having/Set methods.
type Eq map[string]any

func (eq Eq) toSQL(useNotOpr bool) (sql string, args []any, err error) {
	if len(eq) == 0 {
		// Empty Sql{} evaluates to true.
		sql = sqlTrue
		return
	}

	var (
		exprs       []string
		equalOpr    = "="
		inOpr       = "IN"
		nullOpr     = "IS"
		inEmptyExpr = sqlFalse
	)

	if useNotOpr {
		equalOpr = "<>"
		inOpr = "NOT IN"
		nullOpr = "IS NOT"
		inEmptyExpr = sqlTrue
	}

	sortedKeys := getSortedKeys(eq)
	for _, key := range sortedKeys {
		var expr string
		val := eq[key]

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		r := reflect.ValueOf(val)
		if r.Kind() == reflect.Ptr {
			if r.IsNil() {
				val = nil
			} else {
				val = r.Elem().Interface()
			}
		}

		if val == nil {
			expr = fmt.Sprintf("%s %s NULL", key, nullOpr)
		} else {
			if isListType(val) {
				valVal := reflect.ValueOf(val)
				if valVal.Len() == 0 {
					expr = inEmptyExpr
					if args == nil {
						args = []any{}
					}
				} else {
					for i := 0; i < valVal.Len(); i++ {
						args = append(args, valVal.Index(i).Interface())
					}
					expr = fmt.Sprintf("%s %s (%s)", key, inOpr, Placeholders(valVal.Len()))
				}
			} else {
				expr = fmt.Sprintf("%s %s ?", key, equalOpr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr)
	}
	sql = strings.Join(exprs, " AND ")
	return
}

func (eq Eq) SQL() (sql string, args []any, err error) {
	return eq.toSQL(false)
}

// NotEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(NotEq{"id": 1}) == "id <> 1"
type NotEq Eq

func (neq NotEq) SQL() (sql string, args []any, err error) {
	return Eq(neq).toSQL(true)
}

// Like is syntactic sugar for use with LIKE conditions.
// Ex:
//
//	.Where(Like{"name": "%irrel"})
type Like map[string]any

func (lk Like) toSql(opr string) (sql string, args []any, err error) {
	var exprs []string
	for key, val := range lk {
		expr := ""

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			err = fmt.Errorf("cannot use null with like operators")
			return
		} else {
			if isListType(val) {
				err = fmt.Errorf("cannot use array or slice with like operators")
				return
			} else {
				expr = fmt.Sprintf("%s %s ?", key, opr)
				args = append(args, val)
			}
		}
		exprs = append(exprs, expr)
	}
	sql = strings.Join(exprs, " AND ")
	return
}

func (lk Like) SQL() (sql string, args []any, err error) {
	return lk.toSql("LIKE")
}

// NotLike is syntactic sugar for use with LIKE conditions.
// Ex:
//
//	.Where(NotLike{"name": "%irrel"})
type NotLike Like

func (nlk NotLike) SQL() (sql string, args []any, err error) {
	return Like(nlk).toSql("NOT LIKE")
}

// ILike is syntactic sugar for use with ILIKE conditions.
// Ex:
//
//	.Where(ILike{"name": "sq%"})
type ILike Like

func (ilk ILike) SQL() (sql string, args []any, err error) {
	return Like(ilk).toSql("ILIKE")
}

// NotILike is syntactic sugar for use with ILIKE conditions.
// Ex:
//
//	.Where(NotILike{"name": "sq%"})
type NotILike Like

func (nilk NotILike) SQL() (sql string, args []any, err error) {
	return Like(nilk).toSql("NOT ILIKE")
}

// Lt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(Lt{"id": 1})
type Lt map[string]any

func (lt Lt) toSql(opposite, orEq bool) (sql string, args []any, err error) {
	var (
		exprs []string
		opr   = "<"
	)

	if opposite {
		opr = ">"
	}

	if orEq {
		opr = fmt.Sprintf("%s%s", opr, "=")
	}

	sortedKeys := getSortedKeys(lt)
	for _, key := range sortedKeys {
		var expr string
		val := lt[key]

		switch v := val.(type) {
		case driver.Valuer:
			if val, err = v.Value(); err != nil {
				return
			}
		}

		if val == nil {
			err = fmt.Errorf("cannot use null with less than or greater than operators")
			return
		}
		if isListType(val) {
			err = fmt.Errorf("cannot use array or slice with less than or greater than operators")
			return
		}
		expr = fmt.Sprintf("%s %s ?", key, opr)
		args = append(args, val)

		exprs = append(exprs, expr)
	}
	sql = strings.Join(exprs, " AND ")
	return
}

func (lt Lt) SQL() (sql string, args []any, err error) {
	return lt.toSql(false, false)
}

// LtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(LtOrEq{"id": 1}) == "id <= 1"
type LtOrEq Lt

func (ltOrEq LtOrEq) SQL() (sql string, args []any, err error) {
	return Lt(ltOrEq).toSql(false, true)
}

// Gt is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(Gt{"id": 1}) == "id > 1"
type Gt Lt

func (gt Gt) SQL() (sql string, args []any, err error) {
	return Lt(gt).toSql(true, false)
}

// GtOrEq is syntactic sugar for use with Where/Having/Set methods.
// Ex:
//
//	.Where(GtOrEq{"id": 1}) == "id >= 1"
type GtOrEq Lt

func (gtOrEq GtOrEq) SQL() (sql string, args []any, err error) {
	return Lt(gtOrEq).toSql(true, true)
}

type conj []SQLizer

func (c conj) join(sep, defaultExpr string) (sql string, args []any, err error) {
	if len(c) == 0 {
		return defaultExpr, []any{}, nil
	}
	var sqlParts []string
	for _, sqlizer := range c {
		partSQL, partArgs, err := nestedSQL(sqlizer)
		if err != nil {
			return "", nil, err
		}
		if partSQL != "" {
			sqlParts = append(sqlParts, partSQL)
			args = append(args, partArgs...)
		}
	}
	if len(sqlParts) > 0 {
		sql = fmt.Sprintf("(%s)", strings.Join(sqlParts, sep))
	}
	return
}

// And conjunction SQLizers
type And conj

func (a And) SQL() (string, []any, error) {
	return conj(a).join(" AND ", sqlTrue)
}

// Or conjunction SQLizers
type Or conj

func (o Or) SQL() (string, []any, error) {
	return conj(o).join(" OR ", sqlFalse)
}

func getSortedKeys(exp map[string]any) []string {
	sortedKeys := make([]string, 0, len(exp))
	for k := range exp {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	return sortedKeys
}

func isListType(val any) bool {
	if driver.IsValue(val) {
		return false
	}
	valVal := reflect.ValueOf(val)
	return valVal.Kind() == reflect.Array || valVal.Kind() == reflect.Slice
}
