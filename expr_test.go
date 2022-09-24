package pgq

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConcatExpr(t *testing.T) {
	t.Parallel()
	b := ConcatExpr("COALESCE(name,", Expr("CONCAT(?,' ',?)", "f", "l"), ")")
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "COALESCE(name,CONCAT(?,' ',?))"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{"f", "l"}
	assert.Equal(t, expectedArgs, args)
}

func TestConcatExprBadType(t *testing.T) {
	t.Parallel()
	b := ConcatExpr("prefix", 123, "suffix")
	_, _, err := b.SQL()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "123 is not")
}

func TestEqSQL(t *testing.T) {
	t.Parallel()
	b := Eq{"id": 1}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "id = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqEmptySQL(t *testing.T) {
	t.Parallel()
	sql, args, err := Eq{}.SQL()
	assert.NoError(t, err)

	expectedSql := "(1=1)"
	assert.Equal(t, expectedSql, sql)
	assert.Empty(t, args)
}

func TestEqInSQL(t *testing.T) {
	t.Parallel()
	b := Eq{"id": []int{1, 2, 3}}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "id IN (?,?,?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqSQL(t *testing.T) {
	t.Parallel()
	b := NotEq{"id": 1}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "id <> ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestEqNotInSQL(t *testing.T) {
	t.Parallel()
	b := NotEq{"id": []int{1, 2, 3}}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "id NOT IN (?,?,?)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestEqInEmptySQL(t *testing.T) {
	t.Parallel()
	b := Eq{"id": []int{}}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "(1=0)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{}
	assert.Equal(t, expectedArgs, args)
}

func TestNotEqInEmptySQL(t *testing.T) {
	t.Parallel()
	b := NotEq{"id": []int{}}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "(1=1)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{}
	assert.Equal(t, expectedArgs, args)
}

func TestEqBytesSQL(t *testing.T) {
	t.Parallel()
	b := Eq{"id": []byte("test")}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "id = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{[]byte("test")}
	assert.Equal(t, expectedArgs, args)
}

func TestLtSQL(t *testing.T) {
	t.Parallel()
	b := Lt{"id": 1}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "id < ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestLtOrEqSQL(t *testing.T) {
	t.Parallel()
	b := LtOrEq{"id": 1}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "id <= ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtSQL(t *testing.T) {
	t.Parallel()
	b := Gt{"id": 1}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "id > ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestGtOrEqSQL(t *testing.T) {
	t.Parallel()
	b := GtOrEq{"id": 1}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "id >= ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestExprNilSQL(t *testing.T) {
	t.Parallel()
	var b SQLizer
	b = NotEq{"name": nil}
	sql, args, err := b.SQL()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSql := "name IS NOT NULL"
	assert.Equal(t, expectedSql, sql)

	b = Eq{"name": nil}
	sql, args, err = b.SQL()
	assert.NoError(t, err)
	assert.Empty(t, args)

	expectedSql = "name IS NULL"
	assert.Equal(t, expectedSql, sql)
}

func TestNullTypeString(t *testing.T) {
	t.Parallel()
	var b SQLizer
	var name sql.NullString

	b = Eq{"name": name}
	sql, args, err := b.SQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NULL", sql)

	name.Scan("Name")
	b = Eq{"name": name}
	sql, args, err = b.SQL()

	assert.NoError(t, err)
	assert.Equal(t, []any{"Name"}, args)
	assert.Equal(t, "name = ?", sql)
}

func TestNullTypeInt64(t *testing.T) {
	t.Parallel()
	var userID sql.NullInt64
	userID.Scan(nil)
	b := Eq{"user_id": userID}
	sql, args, err := b.SQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "user_id IS NULL", sql)

	userID.Scan(int64(10))
	b = Eq{"user_id": userID}
	sql, args, err = b.SQL()

	assert.NoError(t, err)
	assert.Equal(t, []any{int64(10)}, args)
	assert.Equal(t, "user_id = ?", sql)
}

func TestNilPointer(t *testing.T) {
	t.Parallel()
	var name *string = nil
	eq := Eq{"name": name}
	sql, args, err := eq.SQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NULL", sql)

	neq := NotEq{"name": name}
	sql, args, err = neq.SQL()

	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "name IS NOT NULL", sql)

	var ids *[]int = nil
	eq = Eq{"id": ids}
	sql, args, err = eq.SQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "id IS NULL", sql)

	neq = NotEq{"id": ids}
	sql, args, err = neq.SQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "id IS NOT NULL", sql)

	var ida *[3]int = nil
	eq = Eq{"id": ida}
	sql, args, err = eq.SQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "id IS NULL", sql)

	neq = NotEq{"id": ida}
	sql, args, err = neq.SQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "id IS NOT NULL", sql)

}

func TestNotNilPointer(t *testing.T) {
	t.Parallel()
	c := "Name"
	name := &c
	eq := Eq{"name": name}
	sql, args, err := eq.SQL()

	assert.NoError(t, err)
	assert.Equal(t, []any{"Name"}, args)
	assert.Equal(t, "name = ?", sql)

	neq := NotEq{"name": name}
	sql, args, err = neq.SQL()

	assert.NoError(t, err)
	assert.Equal(t, []any{"Name"}, args)
	assert.Equal(t, "name <> ?", sql)

	s := []int{1, 2, 3}
	ids := &s
	eq = Eq{"id": ids}
	sql, args, err = eq.SQL()
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 2, 3}, args)
	assert.Equal(t, "id IN (?,?,?)", sql)

	neq = NotEq{"id": ids}
	sql, args, err = neq.SQL()
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 2, 3}, args)
	assert.Equal(t, "id NOT IN (?,?,?)", sql)

	a := [3]int{1, 2, 3}
	ida := &a
	eq = Eq{"id": ida}
	sql, args, err = eq.SQL()
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 2, 3}, args)
	assert.Equal(t, "id IN (?,?,?)", sql)

	neq = NotEq{"id": ida}
	sql, args, err = neq.SQL()
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 2, 3}, args)
	assert.Equal(t, "id NOT IN (?,?,?)", sql)
}

func TestEmptyAndSQL(t *testing.T) {
	t.Parallel()
	sql, args, err := And{}.SQL()
	assert.NoError(t, err)

	expectedSql := "(1=1)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{}
	assert.Equal(t, expectedArgs, args)
}

func TestEmptyOrSQL(t *testing.T) {
	t.Parallel()
	sql, args, err := Or{}.SQL()
	assert.NoError(t, err)

	expectedSql := "(1=0)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{}
	assert.Equal(t, expectedArgs, args)
}

func TestLikeSQL(t *testing.T) {
	t.Parallel()
	b := Like{"name": "%irrel"}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "name LIKE ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{"%irrel"}
	assert.Equal(t, expectedArgs, args)
}

func TestNotLikeSQL(t *testing.T) {
	t.Parallel()
	b := NotLike{"name": "%irrel"}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "name NOT LIKE ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{"%irrel"}
	assert.Equal(t, expectedArgs, args)
}

func TestILikeSQL(t *testing.T) {
	t.Parallel()
	b := ILike{"name": "sq%"}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "name ILIKE ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{"sq%"}
	assert.Equal(t, expectedArgs, args)
}

func TestNotILikeSQL(t *testing.T) {
	t.Parallel()
	b := NotILike{"name": "sq%"}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "name NOT ILIKE ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{"sq%"}
	assert.Equal(t, expectedArgs, args)
}

func TestSqlEqOrder(t *testing.T) {
	t.Parallel()
	b := Eq{"a": 1, "b": 2, "c": 3}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "a = ? AND b = ? AND c = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestSqlLtOrder(t *testing.T) {
	t.Parallel()
	b := Lt{"a": 1, "b": 2, "c": 3}
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "a < ? AND b < ? AND c < ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestExprEscaped(t *testing.T) {
	t.Parallel()
	b := Expr("count(??)", Expr("x"))
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql := "count(??)"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{Expr("x")}
	assert.Equal(t, expectedArgs, args)
}

func TestExprRecursion(t *testing.T) {
	t.Parallel()
	{
		b := Expr("count(?)", Expr("nullif(a,?)", "b"))
		sql, args, err := b.SQL()
		assert.NoError(t, err)

		expectedSql := "count(nullif(a,?))"
		assert.Equal(t, expectedSql, sql)

		expectedArgs := []any{"b"}
		assert.Equal(t, expectedArgs, args)
	}
	{
		b := Expr("extract(? from ?)", Expr("epoch"), "2001-02-03")
		sql, args, err := b.SQL()
		assert.NoError(t, err)

		expectedSql := "extract(epoch from ?)"
		assert.Equal(t, expectedSql, sql)

		expectedArgs := []any{"2001-02-03"}
		assert.Equal(t, expectedArgs, args)
	}
	{
		b := Expr("JOIN t1 ON ?", And{Eq{"id": 1}, Expr("NOT c1"), Expr("? @@ ?", "x", "y")})
		sql, args, err := b.SQL()
		assert.NoError(t, err)

		expectedSql := "JOIN t1 ON (id = ? AND NOT c1 AND ? @@ ?)"
		assert.Equal(t, expectedSql, sql)

		expectedArgs := []any{1, "x", "y"}
		assert.Equal(t, expectedArgs, args)
	}
}

func ExampleEq() {
	Select("id", "created", "first_name").From("users").Where(Eq{
		"company": 20,
	})
}
