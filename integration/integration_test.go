package integration

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/henvic/pgq"
	"github.com/henvic/pgtools/sqltest"
	"github.com/jackc/pgx/v5"
)

//go:embed migrations
var mig embed.FS

var force = flag.Bool("force", false, "Force cleaning the database before starting")

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION_TESTDB") != "true" {
		log.Printf("Skipping tests that require database connection")
		return
	}
	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	t.Parallel()

	fsys, err := fs.Sub(mig, "migrations")
	if err != nil {
		t.Fatal(fsys)
	}
	migration := sqltest.New(t, sqltest.Options{
		Force: *force,
		Files: fsys,
	})
	pool := migration.Setup(context.Background(), "")

	s := pgq.Select("v").From("pgq_integration")

	type sqler interface {
		SQL() (sqlStr string, args []any, err error)
	}

	var testCases = []struct {
		name string
		q    sqler
		sql  string
		args []any
		rows []string
	}{
		{
			name: "keq4",
			q:    s.Where(pgq.Eq{"k": 4}),
			sql:  "SELECT v FROM pgq_integration WHERE k = $1",
			args: []any{4},
			rows: []string{"baz"},
		},
		{
			name: "kneq2",
			q:    s.Where(pgq.NotEq{"k": 2}),
			sql:  "SELECT v FROM pgq_integration WHERE k <> $1",
			args: []any{2},
			rows: []string{"foo", "bar", "baz"},
		},
		{
			name: "keq14",
			q:    s.Where(pgq.Eq{"k": []int{1, 4}}),
			sql:  "SELECT v FROM pgq_integration WHERE k = ANY ($1)",
			args: []any{[]int{1, 4}},
			rows: []string{"foo", "baz"},
		},
		{
			name: "kneq14",
			q:    s.Where(pgq.NotEq{"k": []int{1, 4}}),
			sql:  "SELECT v FROM pgq_integration WHERE k <> ALL ($1)",
			args: []any{[]int{1, 4}},
			rows: []string{"bar", "foo"},
		},
		{
			name: "knil",
			q:    s.Where(pgq.Eq{"k": nil}),
			sql:  "SELECT v FROM pgq_integration WHERE k IS NULL",
			rows: []string{},
		},
		{
			name: "knotnil",
			q:    s.Where(pgq.NotEq{"k": nil}),
			sql:  "SELECT v FROM pgq_integration WHERE k IS NOT NULL",
			rows: []string{"foo", "bar", "foo", "baz"},
		},
		{
			name: "keqempty",
			q:    s.Where(pgq.Eq{"k": []int{}}),
			sql:  "SELECT v FROM pgq_integration WHERE (FALSE)",
			rows: []string{},
		},
		{
			name: "knoteqempty",
			q:    s.Where(pgq.NotEq{"k": []int{}}),
			sql:  "SELECT v FROM pgq_integration WHERE (TRUE)",
			rows: []string{"foo", "bar", "foo", "baz"},
		},
		{
			name: "klt3",
			q:    s.Where(pgq.Lt{"k": 3}),
			sql:  "SELECT v FROM pgq_integration WHERE k < $1",
			args: []any{3},
			rows: []string{"foo", "foo"},
		},
		{
			name: "kgt3",
			q:    s.Where(pgq.Gt{"k": 3}),
			sql:  "SELECT v FROM pgq_integration WHERE k > $1",
			args: []any{3},
			rows: []string{"baz"},
		},
		{
			name: "kgt1andlt4",
			q:    s.Where(pgq.And{pgq.Gt{"k": 1}, pgq.Lt{"k": 4}}),
			sql:  "SELECT v FROM pgq_integration WHERE (k > $1 AND k < $2)",
			args: []any{1, 4},
			rows: []string{"bar", "foo"},
		},
		{
			name: "kgt3orlt2",
			q:    s.Where(pgq.Or{pgq.Gt{"k": 3}, pgq.Lt{"k": 2}}),
			sql:  "SELECT v FROM pgq_integration WHERE (k > $1 OR k < $2)",
			args: []any{3, 2},
			rows: []string{"foo", "baz"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sql, args, err := tc.q.SQL()
			if err != nil {
				t.Errorf("expected no error, got %v instead", err)
			}
			if sql != tc.sql {
				t.Errorf("expected %q, got %q instead", tc.sql, sql)
			}
			if !reflect.DeepEqual(args, tc.args) {
				t.Errorf("expected %v, got %v instead", tc.args, args)
			}

			var data []string
			rows, err := pool.Query(context.Background(), sql, args...)
			if err == nil {
				defer rows.Close()
				data, err = pgx.CollectRows(rows, pgx.RowTo[string])
			}
			if err != nil {
				t.Errorf("expected no error, got %v instead", err)
			}

			if !reflect.DeepEqual(data, tc.rows) {
				t.Errorf("expected %v, got %v instead", tc.rows, data)
			}
		})
	}
}

func TestValidQueries(t *testing.T) {
	t.Parallel()

	fsys, err := fs.Sub(mig, "migrations")
	if err != nil {
		t.Fatal(fsys)
	}
	migration := sqltest.New(t, sqltest.Options{
		Force: *force,
		Files: fsys,
	})
	pool := migration.Setup(context.Background(), "")

	type sqler interface {
		SQL() (sql string, args []interface{}, err error)
	}

	testCases := []struct {
		name string
		q    sqler
		want string
	}{
		{
			"select",
			func() sqler {
				caseStmt := pgq.Case("number").
					When("1", "one").
					When("2", "two").
					Else(pgq.Expr("?", "big number"))

				return pgq.Select().
					Column(caseStmt).
					From("atable")
			}(),
			"SELECT CASE number " +
				"WHEN 1 THEN one " +
				"WHEN 2 THEN two " +
				"ELSE $1 " +
				"END " +
				"FROM atable",
		},
		{
			"select_with_alias",
			func() sqler {
				caseStmt := pgq.Case("? > ?", 10, 5).
					When("true", "'T'")

				return pgq.Select().
					Column(pgq.Alias{
						Expr: caseStmt,
						As:   "complexCase",
					}).
					From("atable")
			}(),
			"SELECT (CASE $1 > $2 " +
				"WHEN true THEN 'T' " +
				"END) AS complexCase " +
				"FROM atable",
		},
		{
			"select_with_case",
			func() sqler {
				caseStmt := pgq.Case(pgq.Expr("x = ?", true)).
					When("true", pgq.Expr("?", "it's true!")).
					Else("42")

				return pgq.Select().Column(caseStmt).From("atable")
			}(),
			"SELECT CASE x = $1 " +
				"WHEN true THEN $2 " +
				"ELSE 42 " +
				"END " +
				"FROM atable",
		},
		{
			"select_with_case_and_alias",
			func() sqler {
				caseStmtNoval := pgq.Case(pgq.Expr("x = ?", true)).
					When("true", pgq.Expr("?", "it's true!")).
					Else("42")
				caseStmtExpr := pgq.Case().
					When(pgq.Eq{"x": 0}, "'x is zero'").
					When(pgq.Expr("x > ?", 1), pgq.Expr("CONCAT('x is greater than ', ?)", 2))

				return pgq.Select().
					Column(pgq.Alias{
						Expr: caseStmtNoval,
						As:   "case_noval",
					}).
					Column(pgq.Alias{
						Expr: caseStmtExpr,
						As:   "case_expr",
					}).
					From("atable")
			}(),
			"SELECT " +
				"(CASE x = $1 WHEN true THEN $2 ELSE 42 END) AS case_noval, " +
				"(CASE WHEN x = $3 THEN 'x is zero' WHEN x > $4 THEN CONCAT('x is greater than ', $5) END) AS case_expr " +
				"FROM atable",
		},
		{
			"delete_where_multi",
			func() sqler {
				return pgq.Delete("test").Where("x = ? AND y = ?", 1, 2)
			}(),
			"DELETE FROM test WHERE x = $1 AND y = $2",
		},
		{
			"delete_where_suffix",
			func() sqler {
				return pgq.Delete("test").Where("id=55").Suffix("RETURNING path")
			}(),
			"DELETE FROM test WHERE id=55 RETURNING path",
		},
		{
			"insert_values",
			pgq.Insert("test").Values(1, 2),
			"INSERT INTO test VALUES ($1,$2)",
		},
		{
			"insert_value",
			pgq.Insert("test").Values(1),
			"INSERT INTO test VALUES ($1)",
		},
		{
			"insert_setmap",
			pgq.Insert("atable").SetMap(pgq.Eq{"field1": 1, "field2": 2, "field3": 3}),
			"INSERT INTO atable (field1,field2,field3) VALUES ($1,$2,$3)",
		},
		{
			"insert_with_select",
			func() sqler {
				sb := pgq.Select("field1").From("table1").Where(pgq.Eq{"field1": 1})
				return pgq.Insert("table2").Columns("field1").Select(sb)
			}(),
			"INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = $1",
		},
		{
			"insert_with_returning",
			pgq.Insert("a").
				Columns("b", "c").
				Values(1, 2).
				Values(3, pgq.Expr("? + 1", 4)).
				Returning("abc"),
			"INSERT INTO a (b,c) VALUES ($1,$2),($3,$4 + 1) RETURNING abc",
		},
		{
			"insert_with_returning_multi",
			pgq.Insert("a").
				Columns("b", "c").
				Values(1, 2).
				Values(3, pgq.Expr("? + 1", 4)).
				Returning("abc", "def"),
			"INSERT INTO a (b,c) VALUES ($1,$2),($3,$4 + 1) RETURNING abc, def",
		},
		{
			"insert_with_returning_table_row",
			pgq.Insert("a").
				Columns("b", "c").
				Values(1, 2).
				Values(3, pgq.Expr("? + 1", 4)).
				ReturningSelect(pgq.Select("abc").From("atable"), "something"),
			"INSERT INTO a (b,c) VALUES ($1,$2),($3,$4 + 1) RETURNING (SELECT abc FROM atable) AS something",
		},
		{
			"select_from_select",
			func() sqler {
				subQ := pgq.Select("c").From("d").Where(pgq.Eq{"i": 0})
				return pgq.Select("a", "b").FromSelect(subQ, "subq")
			}(),
			"SELECT a, b FROM (SELECT c FROM d WHERE i = $1) AS subq",
		},
		{
			"select_from_select_2",
			func() sqler {
				subQ := pgq.Select("c").
					From("t").
					Where(pgq.Gt{"c": 1})
				return pgq.Select("c").
					FromSelect(subQ, "subq").
					Where(pgq.Lt{"c": 2})
			}(),
			"SELECT c FROM (SELECT c FROM t WHERE c > $1) AS subq WHERE c < $2",
		},
		{
			"select_where",
			pgq.Select("test").Where("x = ? AND y = ?"),
			"SELECT test WHERE x = $1 AND y = $2",
		},
		{
			"select_from_join",
			pgq.Select("*").From("bar").Join("baz ON bar.foo = baz.foo"),
			"SELECT * FROM bar JOIN baz ON bar.foo = baz.foo",
		},
		{
			"select_from_join_on",
			pgq.Select("*").From("bar").Join("baz ON bar.foo = baz.foo AND baz.foo = ?", 42),
			"SELECT * FROM bar JOIN baz ON bar.foo = baz.foo AND baz.foo = $1",
		},
		{
			"select_from_join_select_on",
			func() sqler {
				nestedSelect := pgq.Select("*").From("baz").Where("foo = ?", 42)
				return pgq.Select("*").From("bar").JoinClause(nestedSelect.Prefix("JOIN (").Suffix(") r ON bar.foo = r.foo"))
			}(),
			"SELECT * FROM bar JOIN ( SELECT * FROM baz WHERE foo = $1 ) r ON bar.foo = r.foo",
		},
		{
			"select_from_option",
			pgq.Select("*").From("foo").Options("DISTINCT"),
			"SELECT DISTINCT * FROM foo",
		},
		{
			"select_from_remove_limit",
			pgq.Select("*").From("foo").Limit(10).RemoveLimit(),
			"SELECT * FROM foo",
		},
		{
			"select_from_remove_offset",
			pgq.Select("*").From("foo").Offset(10).RemoveOffset(),
			"SELECT * FROM foo",
		},
		{
			"select_with_nested_not_exists",
			func() sqler {
				nestedBuilder := pgq.Select("*").Prefix("NOT EXISTS (").
					From("bar").Where("y = ?", 42).Suffix(")")
				return pgq.Select("*").
					From("foo").Where("x = ?").Where(nestedBuilder)
			}(),
			"SELECT * FROM foo WHERE x = $1 AND NOT EXISTS ( SELECT * FROM bar WHERE y = $2 )",
		},
		{
			"select_from_users",
			pgq.Select("*").From("users"),
			"SELECT * FROM users",
		},
		{
			"select_from_users_where_where_nil",
			pgq.Select("*").From("users").Where(nil),
			"SELECT * FROM users",
		},
		{
			"select_from_users_where_where_empty",
			pgq.Select("*").From("users").Where(""),
			"SELECT * FROM users",
		},
		{
			"with_select_from_select",
			func() sqler {
				subquery := pgq.Select("a").Where("b = ?", 1)
				with := subquery.Prefix("WITH a AS (").Suffix(")")
				return pgq.Select("*").
					PrefixExpr(with).
					FromSelect(subquery, "q").
					Where("c = ?", 2)
			}(),
			"WITH a AS ( SELECT a WHERE b = $1 ) SELECT * FROM (SELECT a WHERE b = $2) AS q WHERE c = $3",
		},
		{
			"select_where_exists_select",
			func() sqler {
				subquery := pgq.Select("a").Where(pgq.Eq{"b": 1}).Prefix("EXISTS(").Suffix(")")
				return pgq.Select("*").
					Where(pgq.Or{subquery}).
					Where("c = ?", 2)
			}(),
			"SELECT * WHERE (EXISTS( SELECT a WHERE b = $1 )) AND c = $2",
		},
		{
			"select_join_select_on_where",
			func() sqler {
				subquery := pgq.Select("a").Where(pgq.Eq{"b": 2})

				return pgq.Select("t1.a").
					From("t1").
					Where(pgq.Eq{"a": 1}).
					JoinClause(subquery.Prefix("JOIN (").Suffix(") t2 ON (t1.a = t2.a)"))
			}(),
			"SELECT t1.a FROM t1 JOIN ( SELECT a WHERE b = $1 ) t2 ON (t1.a = t2.a) WHERE a = $2",
		},
		{
			"select_fields_from_table",
			pgq.Select("id", "created", "first_name").From("users"),
			"SELECT id, created, first_name FROM users",
		},
		{
			"select_field_and_count_from_table",
			pgq.Select("first_name", "count(*)").From("users"),
			"SELECT first_name, count(*) FROM users",
		},
		{
			"select_field_and_count_from_table_as",
			pgq.Select("first_name", "count(*) as n_users").From("users"),
			"SELECT first_name, count(*) as n_users FROM users",
		},
		{
			"select_fields_from_table_where",
			pgq.Select("id", "created", "first_name").From("users").
				Where("company = ?", "abc"),
			"SELECT id, created, first_name FROM users WHERE company = $1",
		},
		{
			"select_fields_from_table_where_map",
			pgq.Select("id", "created", "first_name").From("users").
				Where(pgq.Eq{"company": 123}),
			"SELECT id, created, first_name FROM users WHERE company = $1",
		},
		{
			"select_fields_from_table_where_map_gt_or_eq",
			pgq.Select("id", "created", "first_name").From("users").
				Where(pgq.GtOrEq{"created": time.Now().AddDate(0, 0, -7)}),
			"SELECT id, created, first_name FROM users WHERE created >= $1",
		},
		{
			"select_fields_where_multi_maps",
			func() sqler {
				return pgq.Select("id", "created", "first_name").From("users").Where(pgq.And{
					pgq.GtOrEq{
						"created": time.Now().AddDate(0, 0, -7),
					},
					pgq.Eq{
						"company": "whatever",
					},
				})
			}(),
			"SELECT id, created, first_name FROM users WHERE (created >= $1 AND company = $2)",
		},
		{
			"select_fields_from_multi_where",
			func() sqler {
				return pgq.Select("id", "created", "first_name").
					From("users").
					Where("company = ?", "whatever").
					Where(pgq.GtOrEq{
						"created": time.Now().AddDate(0, 0, -7),
					})
			}(),
			"SELECT id, created, first_name FROM users WHERE company = $1 AND created >= $2",
		},
		{
			"multi_select_complex",
			func() sqler {
				usersByCompany := pgq.Select("company", "count(*) as n_users").From("users").GroupBy("company")
				return pgq.Select("company.id", "company.name", "users_by_company.n_users").
					FromSelect(usersByCompany, "users_by_company").
					Join("company on company.id = users_by_company.company")
			}(),
			"SELECT company.id, company.name, users_by_company.n_users FROM (SELECT company, count(*) as n_users FROM users GROUP BY company) AS users_by_company JOIN company on company.id = users_by_company.company",
		},
		{
			"select_columns_from",
			pgq.Select("id").Columns("created", "first_name").From("users"),
			"SELECT id, created, first_name FROM users",
		},
		{
			"select_columns",
			pgq.Select("id").Columns("created").From("users").Columns("first_name"),
			"SELECT id, created, first_name FROM users",
		},
		{
			"select_remove_column",
			func() sqler {
				query := pgq.Select("id").
					From("users").
					RemoveColumns()
				return query.Columns("name")
			}(),
			"SELECT name FROM users",
		},
		{
			"select_table_where",
			pgq.Select("test").Where("x = ?", 1),
			"SELECT test WHERE x = $1",
		},
		{
			"update_where_set",
			pgq.Update("foo").Where("x = ?", "z").Set("a", "b"),
			"UPDATE foo SET a = $1 WHERE x = $2",
		},
		{
			"delete_where",
			pgq.Delete("foo").Where("x = ?", "z"),
			"DELETE FROM foo WHERE x = $1",
		},
		{
			"update_set_map",
			pgq.Update("test").SetMap(pgq.Eq{"x": 1, "y": 2}),
			"UPDATE test SET x = $1, y = $2",
		},
	}
	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sql, _, err := tc.q.SQL()
			if err != nil {
				t.Errorf("expected no error, got %v instead", err)
			}
			if sql != tc.want {
				t.Errorf("expected SQL to be %q, got %q instead", tc.want, sql)
			}
			rows, err := pool.Query(context.Background(), fmt.Sprintf(syntaxCheckTemplate, sql))
			if err == nil {
				defer rows.Close()
				// Forcing reading rows to catch query errors.
				_, err = pgx.CollectRows(rows, pgx.RowToMap)
			}
			if err != nil {
				t.Errorf("expected no error, got %v instead for query %q", err, sql)
			}
		})
	}
}

// syntaxCheckTemplate wraps the query in a construction that returns early.
// This way, we can check the query for correctness without executing it.
// See https://stackoverflow.com/questions/8271606/postgresql-syntax-check-without-running-the-query
const syntaxCheckTemplate = `DO $SYNTAX_CHECK$ BEGIN RETURN; %s; END; $SYNTAX_CHECK$;`
