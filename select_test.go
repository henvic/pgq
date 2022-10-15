package pgq

import (
	"fmt"
	"reflect"
	"testing"
	"time"
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
		Column(Alias{
			Expr: Eq{"b": []int{101, 102, 103}},
			As:   "b_alias",
		}).
		Column(Alias{
			Expr: subQ,
			As:   "subq",
		}).
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
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want :=
		"WITH prefix AS $1 " +
			"SELECT DISTINCT a, b, c, IF(d IN ($2,$3,$4), 1, 0) as stat_column, a > $5, " +
			"(b = ANY ($6)) AS b_alias, " +
			"(SELECT aa, bb FROM dd) AS subq " +
			"FROM e " +
			"CROSS JOIN j1 JOIN j2 LEFT JOIN j3 RIGHT JOIN j4 INNER JOIN j5 CROSS JOIN j6 " +
			"WHERE f = $7 AND g = $8 AND h = $9 AND i = ANY ($10) AND (j = $11 OR (k = $12 AND true)) " +
			"GROUP BY l HAVING m = n ORDER BY $13 DESC, o ASC, p DESC LIMIT 12 OFFSET 13 " +
			"FETCH FIRST $14 ROWS ONLY"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{0, 1, 2, 3, 100, []int{101, 102, 103}, 4, 5, 6, []int{7, 8, 9}, 10, 11, 1, 14}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestSelectBuilderFromSelect(t *testing.T) {
	t.Parallel()
	subQ := Select("c").From("d").Where(Eq{"i": 0})
	b := Select("a", "b").FromSelect(subQ, "subq")
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT a, b FROM (SELECT c FROM d WHERE i = $1) AS subq"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{0}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestSelectBuilderFromSelectNestedDollarPlaceholders(t *testing.T) {
	t.Parallel()
	subQ := Select("c").
		From("t").
		Where(Gt{"c": 1})
	b := Select("c").
		FromSelect(subQ, "subq").
		Where(Lt{"c": 2})
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT c FROM (SELECT c FROM t WHERE c > $1) AS subq WHERE c < $2"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1, 2}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestSelectBuilderSQLErr(t *testing.T) {
	t.Parallel()
	_, _, err := Select().From("x").SQL()

	want := "select statements must have at least one result column"
	if err.Error() != want {
		t.Errorf("expected error to be %q, got %q instead", want, err)
	}
}

func TestSelectBuilderPlaceholders(t *testing.T) {
	t.Parallel()
	b := Select("test").Where("x = ? AND y = ?")

	sql, _, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT test WHERE x = $1 AND y = $2"; sql != want {
		t.Errorf("expected %q, got %q instead", want, sql)
	}
}

func TestSelectBuilderSimpleJoin(t *testing.T) {
	t.Parallel()
	want := "SELECT * FROM bar JOIN baz ON bar.foo = baz.foo"
	expectedArgs := []any(nil)

	b := Select("*").From("bar").Join("baz ON bar.foo = baz.foo")

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("expected %v, got %v instead", expectedArgs, args)
	}
}

func TestSelectBuilderParamJoin(t *testing.T) {
	t.Parallel()
	want := "SELECT * FROM bar JOIN baz ON bar.foo = baz.foo AND baz.foo = $1"
	expectedArgs := []any{42}

	b := Select("*").From("bar").Join("baz ON bar.foo = baz.foo AND baz.foo = ?", 42)

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("expected %v, got %v instead", expectedArgs, args)
	}
}

func TestSelectBuilderNestedSelectJoin(t *testing.T) {
	t.Parallel()
	want := "SELECT * FROM bar JOIN ( SELECT * FROM baz WHERE foo = $1 ) r ON bar.foo = r.foo"
	expectedArgs := []any{42}

	nestedSelect := Select("*").From("baz").Where("foo = ?", 42)

	b := Select("*").From("bar").JoinClause(nestedSelect.Prefix("JOIN (").Suffix(") r ON bar.foo = r.foo"))

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("expected %v, got %v instead", expectedArgs, args)
	}
}

func TestSelectWithOptions(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("foo").Distinct().Options("SQL_NO_CACHE").SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT DISTINCT SQL_NO_CACHE * FROM foo"; sql != want {
		t.Errorf("expected %q, got %v", want, sql)
	}
}

func TestSelectWithRemoveLimit(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("foo").Limit(10).RemoveLimit().SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT * FROM foo"; sql != want {
		t.Errorf("expected %q, got %v", want, sql)
	}
}

func TestSelectWithRemoveOffset(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("foo").Offset(10).RemoveOffset().SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT * FROM foo"; sql != want {
		t.Errorf("expected %q, got %v", want, sql)
	}
}

func TestSelectBuilderNestedSelectDollar(t *testing.T) {
	t.Parallel()
	nestedBuilder := Select("*").Prefix("NOT EXISTS (").
		From("bar").Where("y = ?", 42).Suffix(")")
	outerSQL, _, err := Select("*").
		From("foo").Where("x = ?").Where(nestedBuilder).SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT * FROM foo WHERE x = $1 AND NOT EXISTS ( SELECT * FROM bar WHERE y = $2 )"; outerSQL != want {
		t.Errorf("expected %q, got %v", want, outerSQL)
	}
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
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT * FROM users"; sql != want {
		t.Errorf("expected %q, got %v", want, sql)
	}
}

func TestSelectWithNilWhereClause(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("users").Where(nil).SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT * FROM users"; sql != want {
		t.Errorf("expected %q, got %v", want, sql)
	}
}

func TestSelectWithEmptyStringWhereClause(t *testing.T) {
	t.Parallel()
	sql, _, err := Select("*").From("users").Where("").SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT * FROM users"; sql != want {
		t.Errorf("expected %q, got %v", want, sql)
	}
}

func TestSelectSubqueryPlaceholderNumbering(t *testing.T) {
	t.Parallel()
	subquery := Select("a").Where("b = ?", 1)
	with := subquery.Prefix("WITH a AS (").Suffix(")")

	sql, args, err := Select("*").
		PrefixExpr(with).
		FromSelect(subquery, "q").
		Where("c = ?", 2).
		SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "WITH a AS ( SELECT a WHERE b = $1 ) SELECT * FROM (SELECT a WHERE b = $2) AS q WHERE c = $3"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	if want := []any{1, 1, 2}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected %q, got %q instead", want, args)
	}
}

func TestSelectSubqueryInConjunctionPlaceholderNumbering(t *testing.T) {
	t.Parallel()
	subquery := Select("a").Where(Eq{"b": 1}).Prefix("EXISTS(").Suffix(")")

	sql, args, err := Select("*").
		Where(Or{subquery}).
		Where("c = ?", 2).
		SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT * WHERE (EXISTS( SELECT a WHERE b = $1 )) AND c = $2"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	if want := []any{1, 2}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected %q, got %q instead", want, args)
	}
}

func TestSelectJoinClausePlaceholderNumbering(t *testing.T) {
	t.Parallel()
	subquery := Select("a").Where(Eq{"b": 2})

	sql, args, err := Select("t1.a").
		From("t1").
		Where(Eq{"a": 1}).
		JoinClause(subquery.Prefix("JOIN (").Suffix(") t2 ON (t1.a = t2.a)")).
		SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT t1.a FROM t1 JOIN ( SELECT a WHERE b = $1 ) t2 ON (t1.a = t2.a) WHERE a = $2"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	if want := []any{2, 1}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected %q, got %q instead", want, args)
	}
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
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT name FROM users"; sql != want {
		t.Errorf("expected %q, got %v", want, sql)
	}
}
