package integration

import (
	"context"
	"embed"
	"flag"
	"io/fs"
	"log"
	"os"
	"reflect"
	"testing"

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
			sql:  "SELECT v FROM pgq_integration WHERE (FALSE)",
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
