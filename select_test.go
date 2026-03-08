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
		OrderBy("a ASC").
		RemoveOrderBy().
		OrderByClause("? DESC", 1).
		OrderBy("o ASC", "p DESC").
		Limit(12).
		Offset(13).
		Suffix("FETCH FIRST ? ROWS ONLY", 14)

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "WITH prefix AS $1 " +
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
	sql, _, err := Select("*").From("foo").Options("ALL").SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "SELECT ALL * FROM foo"; sql != want {
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

func TestSelectBuilderColumnAliasSubqueryParams(t *testing.T) {
	t.Parallel()
	subQ := Select("name").From("producers").Where("active = ?", true)
	b := Select("id").
		Column(Alias{Expr: subQ, As: "producer_name"}).
		From("films").
		Where("id = ?", 42)
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT id, (SELECT name FROM producers WHERE active = $1) AS producer_name FROM films WHERE id = $2"
	if sql != want {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{true, 42}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("wanted %v, got %v instead", expectedArgs, args)
	}
}

func TestSelectBuilder_PrefixExpr_NestedUpdateDollar(t *testing.T) {
	t.Parallel()
	nestedBuilder := Update("foo").Prefix("WITH updated AS (").
		Set("x", 42).Where("x = ?", 41).Returning("*").Suffix(")")
	outerSQL, _, err := Select("*").
		From("updated").Where("y = ?", 11).PrefixExpr(nestedBuilder).SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	want := "WITH updated AS ( UPDATE foo SET x = $1 WHERE x = $2 RETURNING * ) SELECT * FROM updated WHERE y = $3"
	if outerSQL != want {
		t.Errorf("expected %q, got %v", want, outerSQL)
	}
}

func TestSelectBuilder_PrefixExpr_NestedDeleteDollar(t *testing.T) {
	t.Parallel()
	nestedBuilder := Delete("foo").Prefix("WITH deleted AS (").
		Where("x = ?", 41).Returning("*").Suffix(")")
	outerSQL, _, err := Select("*").
		From("deleted").Where("y = ?", 11).PrefixExpr(nestedBuilder).SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	want := "WITH deleted AS ( DELETE FROM foo WHERE x = $1 RETURNING * ) SELECT * FROM deleted WHERE y = $2"
	if outerSQL != want {
		t.Errorf("expected %q, got %v", want, outerSQL)
	}
}

func TestSelectBuilderWith(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		b        SelectBuilder
		wantSQL  string
		wantArgs []any
	}{
		{
			name: "with_cte",
			b: Select("region", "product", "SUM(quantity) AS product_units", "SUM(amount) AS product_sales").
				With("regional_sales", Select("region", "SUM(amount) AS total_sales").
					From("orders").
					GroupBy("region")).
				With("top_regions", Select("region").
					From("regional_sales").
					Where("total_sales > (SELECT SUM(total_sales)/10 FROM regional_sales)")).
				From("orders").
				Where("region IN (SELECT region FROM top_regions)").
				GroupBy("region", "product"),
			wantSQL: "WITH regional_sales AS (SELECT region, SUM(amount) AS total_sales FROM orders GROUP BY region), " +
				"top_regions AS (SELECT region FROM regional_sales WHERE total_sales > (SELECT SUM(total_sales)/10 FROM regional_sales)) " +
				"SELECT region, product, SUM(quantity) AS product_units, SUM(amount) AS product_sales " +
				"FROM orders WHERE region IN (SELECT region FROM top_regions) GROUP BY region, product",
			wantArgs: nil,
		},
		{
			name: "cte with args",
			b: Select("id", "total").
				With("totals", Select("id", "SUM(amount) AS total").From("orders").Where("amount > ?", 100).GroupBy("id")).
				From("totals").
				Where("total > ?", 500),
			wantSQL: "WITH totals AS (SELECT id, SUM(amount) AS total FROM orders WHERE amount > $1 GROUP BY id) " +
				"SELECT id, total FROM totals WHERE total > $2",
			wantArgs: []any{100, 500},
		},
		{
			name: "cte body is insert returning",
			b: Select("*").
				With("inserted", Insert("orders").
					Columns("region", "amount").
					Values("West", 42).
					Returning("id")).
				From("inserted"),
			wantSQL: "WITH inserted AS (INSERT INTO orders (region,amount) VALUES ($1,$2) RETURNING id) " +
				"SELECT * FROM inserted",
			wantArgs: []any{"West", 42},
		},
		{
			name: "cte body is update returning",
			b: Select("*").
				With("moved", Update("old_table").Set("status", "archived").Where("id = ?", 42).Returning("*")).
				From("moved"),
			wantSQL: "WITH moved AS (UPDATE old_table SET status = $1 WHERE id = $2 RETURNING *) " +
				"SELECT * FROM moved",
			wantArgs: []any{"archived", 42},
		},
		{
			name: "cte body is delete returning",
			b: Select("*").
				With("removed", Delete("old_table").Where("id = ?", 7).Returning("*")).
				From("removed"),
			wantSQL: "WITH removed AS (DELETE FROM old_table WHERE id = $1 RETURNING *) " +
				"SELECT * FROM removed",
			wantArgs: []any{7},
		},
		{
			name: "cte with prefix coexist",
			b: Select("*").
				With("c", Select("id").From("t").Where("x = ?", 1)).
				Prefix("/* hint */").
				From("c"),
			wantSQL:  "WITH c AS (SELECT id FROM t WHERE x = $1) /* hint */ SELECT * FROM c",
			wantArgs: []any{1},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sql, args, err := tc.b.SQL()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sql != tc.wantSQL {
				t.Errorf("expected SQL to be %q, got %q instead", tc.wantSQL, sql)
			}
			if !reflect.DeepEqual(args, tc.wantArgs) {
				t.Errorf("expected args %v, got %v instead", tc.wantArgs, args)
			}
		})
	}
}

func TestSelectBuilderWithRecursive(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		b        SelectBuilder
		wantSQL  string
		wantArgs []any
	}{
		{
			name: "recursive query",
			b: Select("sub_part", "SUM(quantity) as total_quantity").
				WithRecursive("included_parts(sub_part, part, quantity)", UnionAll(
					Select("sub_part", "part", "quantity").From("parts").Where("part = ?", "our_product"),
					Select("p.sub_part", "p.part", "p.quantity * pr.quantity").From("included_parts pr, parts p").Where("p.part = pr.sub_part"),
				)).
				From("included_parts").
				GroupBy("sub_part"),
			wantSQL: "WITH RECURSIVE included_parts(sub_part, part, quantity) AS (" +
				"SELECT sub_part, part, quantity FROM parts WHERE part = $1 " +
				"UNION ALL " +
				"SELECT p.sub_part, p.part, p.quantity * pr.quantity FROM included_parts pr, parts p WHERE p.part = pr.sub_part) " +
				"SELECT sub_part, SUM(quantity) as total_quantity FROM included_parts GROUP BY sub_part",
			wantArgs: []any{"our_product"},
		},
		{
			name: "search tree",
			b: Select("*").
				WithRecursive("search_tree(id, link, data)", UnionAll(
					Select("t.id", "t.link", "t.data").From("tree t"),
					Select("t.id", "t.link", "t.data").From("tree t, search_tree st").Where("t.id = st.link"),
				)).
				From("search_tree"),
			wantSQL: "WITH RECURSIVE search_tree(id, link, data) AS (" +
				"SELECT t.id, t.link, t.data FROM tree t " +
				"UNION ALL " +
				"SELECT t.id, t.link, t.data FROM tree t, search_tree st WHERE t.id = st.link" +
				") SELECT * FROM search_tree",
		},
		{
			name: "mix With and WithRecursive emits RECURSIVE",
			b: Select("*").
				With("base", Select("id").From("t")).
				WithRecursive("tree",
					UnionAll(
						Select("id").From("nodes").Where("parent IS NULL"),
						Select("n.id").From("nodes n").Join("tree ON tree.id = n.parent"),
					),
				).
				From("tree"),
			wantSQL: "WITH RECURSIVE " +
				"base AS (SELECT id FROM t), " +
				"tree AS (SELECT id FROM nodes WHERE parent IS NULL UNION ALL SELECT n.id FROM nodes n JOIN tree ON tree.id = n.parent) " +
				"SELECT * FROM tree",
		},
		{
			name: "only With calls do not emit RECURSIVE",
			b: Select("*").
				With("a", Select("id").From("t1")).
				With("b", Select("id").From("t2")).
				From("a"),
			wantSQL: "WITH a AS (SELECT id FROM t1), b AS (SELECT id FROM t2) SELECT * FROM a",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sql, args, err := tc.b.SQL()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sql != tc.wantSQL {
				t.Errorf("expected SQL to be %q, got %q instead", tc.wantSQL, sql)
			}
			if !reflect.DeepEqual(args, tc.wantArgs) {
				t.Errorf("expected args %v, got %v instead", tc.wantArgs, args)
			}
		})
	}
}

func TestUnion(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		u        UnionBuilder
		wantSQL  string
		wantArgs []any
	}{
		{
			name:    "union no args",
			u:       Union(Select("id", "name").From("customers"), Select("id", "name").From("employees")),
			wantSQL: "SELECT id, name FROM customers UNION SELECT id, name FROM employees",
		},
		{
			name:    "union all no args",
			u:       UnionAll(Select("id", "name").From("customers"), Select("id", "name").From("employees")),
			wantSQL: "SELECT id, name FROM customers UNION ALL SELECT id, name FROM employees",
		},
		{
			name: "union with args",
			u: Union(
				Select("id").From("t").Where("x = ?", 1),
				Select("id").From("t").Where("x = ?", 2),
			),
			wantSQL:  "SELECT id FROM t WHERE x = $1 UNION SELECT id FROM t WHERE x = $2",
			wantArgs: []any{1, 2},
		},
		{
			name: "union all with args",
			u: UnionAll(
				Select("id").From("t").Where("x = ?", 1),
				Select("id").From("t").Where("x = ?", 2),
			),
			wantSQL:  "SELECT id FROM t WHERE x = $1 UNION ALL SELECT id FROM t WHERE x = $2",
			wantArgs: []any{1, 2},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sql, args, err := tc.u.SQL()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sql != tc.wantSQL {
				t.Errorf("expected SQL to be %q, got %q instead", tc.wantSQL, sql)
			}
			if !reflect.DeepEqual(args, tc.wantArgs) {
				t.Errorf("expected args %v, got %v instead", tc.wantArgs, args)
			}
		})
	}
}
