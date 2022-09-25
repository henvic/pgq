package pgq

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSelectBuilderSQL(t *testing.T) {
	t.Parallel()
	subQ := Select("aa", "bb").From("dd")
	b := Select("a", "b").
		Prefix("WITH prefix AS ?", 0).
		Distinct().
		Columns("c").
		Column("IF(d IN ("+Placeholders(3)+"), 1, 0) as stat_column", 1, 2, 3).
		Column(Expr("a > ?", 100)).
		Column(Alias(Eq{"b": []int{101, 102, 103}}, "b_alias")).
		Column(Alias(subQ, "subq")).
		From("e").
		JoinClause("CROSS JOIN j1").
		Join("j2").
		LeftJoin("j3").
		RightJoin("j4").
		InnerJoin("j5").
		CrossJoin("j6").
		Where("f = ?", 4).
		Where(Eq{"g": 5}).
		Where(map[string]any{"h": 6}).
		Where(Eq{"i": []int{7, 8, 9}}).
		Where(Or{Expr("j = ?", 10), And{Eq{"k": 11}, Expr("true")}}).
		GroupBy("l").
		Having("m = n").
		OrderByClause("? DESC", 1).
		OrderBy("o ASC", "p DESC").
		Limit(12).
		Offset(13).
		Suffix("FETCH FIRST ? ROWS ONLY", 14)

	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSQL :=
		"WITH prefix AS $1 " +
			"SELECT DISTINCT a, b, c, IF(d IN ($2,$3,$4), 1, 0) as stat_column, a > $5, " +
			"(b IN ($6,$7,$8)) AS b_alias, " +
			"(SELECT aa, bb FROM dd) AS subq " +
			"FROM e " +
			"CROSS JOIN j1 JOIN j2 LEFT JOIN j3 RIGHT JOIN j4 INNER JOIN j5 CROSS JOIN j6 " +
			"WHERE f = $9 AND g = $10 AND h = $11 AND i IN ($12,$13,$14) AND (j = $15 OR (k = $16 AND true)) " +
			"GROUP BY l HAVING m = n ORDER BY $17 DESC, o ASC, p DESC LIMIT 12 OFFSET 13 " +
			"FETCH FIRST $18 ROWS ONLY"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 1, 2, 3, 100, 101, 102, 103, 4, 5, 6, 7, 8, 9, 10, 11, 1, 14}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderFromSelect(t *testing.T) {
	t.Parallel()
	subQ := Select("c").From("d").Where(Eq{"i": 0})
	b := Select("a", "b").FromSelect(subQ, "subq")
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT a, b FROM (SELECT c FROM d WHERE i = $1) AS subq"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderFromSelectNestedDollarPlaceholders(t *testing.T) {
	t.Parallel()
	subQ := Select("c").
		From("t").
		Where(Gt{"c": 1}).
		PlaceholderFormat(Dollar)
	b := Select("c").
		FromSelect(subQ, "subq").
		Where(Lt{"c": 2}).
		PlaceholderFormat(Dollar)
	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT c FROM (SELECT c FROM t WHERE c > $1) AS subq WHERE c < $2"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestSelectBuilderSQLErr(t *testing.T) {
	t.Parallel()
	_, _, err := Select().From("x").SQL()
	assert.Error(t, err)
}

func TestSelectBuilderPlaceholders(t *testing.T) {
	t.Parallel()
	b := Select("test").Where("x = ? AND y = ?")

	sql, _, _ := b.PlaceholderFormat(Question).SQL()
	assert.Equal(t, "SELECT test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).SQL()
	assert.Equal(t, "SELECT test WHERE x = $1 AND y = $2", sql)
}

func TestSelectBuilderSimpleJoin(t *testing.T) {
	t.Parallel()
	expectedSQL := "SELECT * FROM bar JOIN baz ON bar.foo = baz.foo"
	expectedArgs := []any(nil)

	b := Select("*").From("bar").Join("baz ON bar.foo = baz.foo")

	sql, args, err := b.SQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, args, expectedArgs)
}

func TestSelectBuilderParamJoin(t *testing.T) {
	t.Parallel()
	expectedSQL := "SELECT * FROM bar JOIN baz ON bar.foo = baz.foo AND baz.foo = $1"
	expectedArgs := []any{42}

	b := Select("*").From("bar").Join("baz ON bar.foo = baz.foo AND baz.foo = ?", 42)

	sql, args, err := b.SQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, args, expectedArgs)
}

func TestSelectBuilderNestedSelectJoin(t *testing.T) {
	t.Parallel()
	expectedSQL := "SELECT * FROM bar JOIN ( SELECT * FROM baz WHERE foo = $1 ) r ON bar.foo = r.foo"
	expectedArgs := []any{42}

	nestedSelect := Select("*").From("baz").Where("foo = ?", 42)

	b := Select("*").From("bar").JoinClause(nestedSelect.Prefix("JOIN (").Suffix(") r ON bar.foo = r.foo"))

	sql, args, err := b.SQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, args, expectedArgs)
}

func TestSelectWithOptions(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("foo").Distinct().Options("SQL_NO_CACHE").SQL()

	assert.NoError(t, err)
	assert.Equal(t, "SELECT DISTINCT SQL_NO_CACHE * FROM foo", sql)
}

func TestSelectWithRemoveLimit(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("foo").Limit(10).RemoveLimit().SQL()

	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM foo", sql)
}

func TestSelectWithRemoveOffset(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("foo").Offset(10).RemoveOffset().SQL()

	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM foo", sql)
}

func TestSelectBuilderNestedSelectDollar(t *testing.T) {
	t.Parallel()
	nestedBuilder := StatementBuilder.PlaceholderFormat(Dollar).Select("*").Prefix("NOT EXISTS (").
		From("bar").Where("y = ?", 42).Suffix(")")
	outerSQL, _, err := StatementBuilder.PlaceholderFormat(Dollar).Select("*").
		From("foo").Where("x = ?").Where(nestedBuilder).SQL()

	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM foo WHERE x = $1 AND NOT EXISTS ( SELECT * FROM bar WHERE y = $2 )", outerSQL)
}

func TestSelectBuilderMustSQL(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestSelectBuilderMustSQL should have panicked!")
		}
	}()
	// This function should cause a panic
	Select().From("foo").MustSQL()
}

func TestSelectWithoutWhereClause(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("users").SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
}

func TestSelectWithNilWhereClause(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("users").Where(nil).SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
}

func TestSelectWithEmptyStringWhereClause(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("users").Where("").SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", sql)
}

func TestSelectSubqueryPlaceholderNumbering(t *testing.T) {
	t.Parallel()
	subquery := Select("a").Where("b = ?", 1).PlaceholderFormat(Dollar)
	with := subquery.Prefix("WITH a AS (").Suffix(")")

	sql, args, err := Select("*").
		PrefixExpr(with).
		FromSelect(subquery, "q").
		Where("c = ?", 2).
		PlaceholderFormat(Dollar).
		SQL()
	assert.NoError(t, err)

	expectedSQL := "WITH a AS ( SELECT a WHERE b = $1 ) SELECT * FROM (SELECT a WHERE b = $2) AS q WHERE c = $3"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, 1, 2}, args)
}

func TestSelectSubqueryInConjunctionPlaceholderNumbering(t *testing.T) {
	t.Parallel()
	subquery := Select("a").Where(Eq{"b": 1}).Prefix("EXISTS(").Suffix(")").PlaceholderFormat(Dollar)

	sql, args, err := Select("*").
		Where(Or{subquery}).
		Where("c = ?", 2).
		PlaceholderFormat(Dollar).
		SQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT * WHERE (EXISTS( SELECT a WHERE b = $1 )) AND c = $2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestSelectJoinClausePlaceholderNumbering(t *testing.T) {
	t.Parallel()
	subquery := Select("a").Where(Eq{"b": 2}).PlaceholderFormat(Dollar)

	sql, args, err := Select("t1.a").
		From("t1").
		Where(Eq{"a": 1}).
		JoinClause(subquery.Prefix("JOIN (").Suffix(") t2 ON (t1.a = t2.a)")).
		PlaceholderFormat(Dollar).
		SQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT t1.a FROM t1 JOIN ( SELECT a WHERE b = $1 ) t2 ON (t1.a = t2.a) WHERE a = $2"
	assert.Equal(t, expectedSQL, sql)
	assert.Equal(t, []any{2, 1}, args)
}

func ExampleSelect() {
	Select("id", "created", "first_name").From("users") // ... continue building up your query

	// sql methods in select columns are ok
	Select("first_name", "count(*)").From("users")

	// column aliases are ok too
	Select("first_name", "count(*) as n_users").From("users")
}

func ExampleSelectBuilder_From() {
	Select("id", "created", "first_name").From("users") // ... continue building up your query
}

func ExampleSelectBuilder_Where() {
	companyId := 20
	Select("id", "created", "first_name").From("users").Where("company = ?", companyId)
}

func ExampleSelectBuilder_Where_helpers() {
	companyId := 20

	Select("id", "created", "first_name").From("users").Where(Eq{
		"company": companyId,
	})

	Select("id", "created", "first_name").From("users").Where(GtOrEq{
		"created": time.Now().AddDate(0, 0, -7),
	})

	Select("id", "created", "first_name").From("users").Where(And{
		GtOrEq{
			"created": time.Now().AddDate(0, 0, -7),
		},
		Eq{
			"company": companyId,
		},
	})
}

func ExampleSelectBuilder_Where_multiple() {
	companyId := 20

	// multiple where's are ok

	Select("id", "created", "first_name").
		From("users").
		Where("company = ?", companyId).
		Where(GtOrEq{
			"created": time.Now().AddDate(0, 0, -7),
		})
}

func ExampleSelectBuilder_FromSelect() {
	usersByCompany := Select("company", "count(*) as n_users").From("users").GroupBy("company")
	query := Select("company.id", "company.name", "users_by_company.n_users").
		FromSelect(usersByCompany, "users_by_company").
		Join("company on company.id = users_by_company.company")

	sql, _, _ := query.SQL()
	fmt.Println(sql)

	// Output: SELECT company.id, company.name, users_by_company.n_users FROM (SELECT company, count(*) as n_users FROM users GROUP BY company) AS users_by_company JOIN company on company.id = users_by_company.company
}

func ExampleSelectBuilder_Columns() {
	query := Select("id").Columns("created", "first_name").From("users")

	sql, _, _ := query.SQL()
	fmt.Println(sql)
	// Output: SELECT id, created, first_name FROM users
}

func ExampleSelectBuilder_Columns_order() {
	// out of order is ok too
	query := Select("id").Columns("created").From("users").Columns("first_name")

	sql, _, _ := query.SQL()
	fmt.Println(sql)
	// Output: SELECT id, created, first_name FROM users
}

func TestRemoveColumns(t *testing.T) {
	t.Parallel()
	query := Select("id").
		From("users").
		RemoveColumns()
	query = query.Columns("name")
	sql, _, err := query.SQL()
	assert.NoError(t, err)
	assert.Equal(t, "SELECT name FROM users", sql)
}
