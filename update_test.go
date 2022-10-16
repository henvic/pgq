package pgq

import (
	"reflect"
	"testing"
)

func TestUpdateBuilderSQL(t *testing.T) {
	t.Parallel()
	beginning := Update("").
		Prefix("WITH prefix AS ?", 0).
		Table("a").
		Set("b", Expr("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Set("c1", Case("status").When("1", "2").When("2", "1")).
		Set("c2", Case().When("a = 2", Expr("?", "foo")).When("a = 3", Expr("?", "bar"))).
		Set("c3", Select("a").From("b")).
		Where("d = ?", 3).
		OrderBy("e")

	testCases := []struct {
		name     string
		b        UpdateBuilder
		wantSQL  string
		wantArgs []any
		wantErr  error
	}{
		{
			name: "with_suffix",
			b:    beginning.Suffix("RETURNING ?", 6),
			wantSQL: "WITH prefix AS $1 " +
				"UPDATE a SET b = $2 + 1, c = $3, " +
				"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
				"c2 = CASE WHEN a = 2 THEN $4 WHEN a = 3 THEN $5 END, " +
				"c3 = (SELECT a FROM b) " +
				"WHERE d = $6 " +
				"ORDER BY e " +
				"RETURNING $7",
			wantArgs: []any{0, 1, 2, "foo", "bar", 3, 6},
		},
		{
			name: "returning",
			b:    beginning.Returning("x"),
			wantSQL: "WITH prefix AS $1 " +
				"UPDATE a SET b = $2 + 1, c = $3, " +
				"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
				"c2 = CASE WHEN a = 2 THEN $4 WHEN a = 3 THEN $5 END, " +
				"c3 = (SELECT a FROM b) " +
				"WHERE d = $6 " +
				"ORDER BY e " +
				"RETURNING x",
			wantArgs: []any{0, 1, 2, "foo", "bar", 3},
		},
		{
			name: "returning_2",
			b:    beginning.Returning("x", "y"),
			wantSQL: "WITH prefix AS $1 " +
				"UPDATE a SET b = $2 + 1, c = $3, " +
				"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
				"c2 = CASE WHEN a = 2 THEN $4 WHEN a = 3 THEN $5 END, " +
				"c3 = (SELECT a FROM b) " +
				"WHERE d = $6 " +
				"ORDER BY e " +
				"RETURNING x, y",
			wantArgs: []any{0, 1, 2, "foo", "bar", 3},
		},
		{
			name: "returning_3",
			b:    beginning.Returning("x", "y", "z"),
			wantSQL: "WITH prefix AS $1 " +
				"UPDATE a SET b = $2 + 1, c = $3, " +
				"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
				"c2 = CASE WHEN a = 2 THEN $4 WHEN a = 3 THEN $5 END, " +
				"c3 = (SELECT a FROM b) " +
				"WHERE d = $6 " +
				"ORDER BY e " +
				"RETURNING x, y, z",
			wantArgs: []any{0, 1, 2, "foo", "bar", 3},
		},
		{
			name: "returning_3_multi_calls",
			b:    beginning.Returning("x", "y").Returning("z"),
			wantSQL: "WITH prefix AS $1 " +
				"UPDATE a SET b = $2 + 1, c = $3, " +
				"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
				"c2 = CASE WHEN a = 2 THEN $4 WHEN a = 3 THEN $5 END, " +
				"c3 = (SELECT a FROM b) " +
				"WHERE d = $6 " +
				"ORDER BY e " +
				"RETURNING x, y, z",
			wantArgs: []any{0, 1, 2, "foo", "bar", 3},
		},
		{
			name: "returning_select",
			b:    beginning.ReturningSelect(Select("abc").From("atable"), "something"),
			wantSQL: "WITH prefix AS $1 " +
				"UPDATE a SET b = $2 + 1, c = $3, " +
				"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
				"c2 = CASE WHEN a = 2 THEN $4 WHEN a = 3 THEN $5 END, " +
				"c3 = (SELECT a FROM b) " +
				"WHERE d = $6 " +
				"ORDER BY e " +
				"RETURNING (SELECT abc FROM atable) AS something",
			wantArgs: []any{0, 1, 2, "foo", "bar", 3},
		},
		{
			name: "from",
			b: Update("employees").Set("sales_count", Expr("sales_count + 1")).From("accounts").
				Where("accounts.name = ?", "Acme Corporation").
				Where("employees.id = accounts.sales_person"),
			wantSQL: "UPDATE employees SET sales_count = sales_count + 1 FROM accounts " +
				"WHERE accounts.name = $1 " +
				"AND employees.id = accounts.sales_person",
			wantArgs: []any{"Acme Corporation"},
		},
		{
			name: "from_select",
			b: Update("employees").Set("sales_count", Expr("sales_count + 1")).FromSelect(
				Select("name").From("accounts"), "acc",
			).
				Where("acc.name = ?", "Acme Corporation").
				Where("employees.id = acc.sales_person"),
			wantSQL: "UPDATE employees SET sales_count = sales_count + 1 " +
				"FROM (SELECT name FROM accounts) " +
				"AS acc WHERE acc.name = $1 AND employees.id = acc.sales_person",
			wantArgs: []any{"Acme Corporation"},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sql, args, err := tc.b.SQL()
			if err != tc.wantErr {
				t.Errorf("expected error to be %v, got %v instead", tc.wantErr, err)
			}
			if sql != tc.wantSQL {
				t.Errorf("expected SQL to be %q, got %q instead", tc.wantSQL, sql)
			}
			if !reflect.DeepEqual(args, tc.wantArgs) {
				t.Errorf("wanted %v, got %v instead", tc.wantArgs, args)
			}
		})
	}
}

func TestUpdateBuilderSQLErr(t *testing.T) {
	t.Parallel()
	_, _, err := Update("").Set("x", 1).SQL()
	if want := "update statements must specify a table"; err.Error() != want {
		t.Errorf("expected error to be %q, got %q instead", want, err)
	}

	_, _, err = Update("x").SQL()
	if want := "update statements must have at least one Set clause"; err.Error() != want {
		t.Errorf("expected error to be %q, got %q instead", want, err)
	}
}

func TestUpdateBuilderMustSQL(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestUpdateBuilderMustSQL should have panicked!")
		}
	}()
	Update("").MustSQL()
}

func TestUpdateBuilderPlaceholders(t *testing.T) {
	t.Parallel()
	b := Update("test").SetMap(Eq{"x": 1, "y": 2})

	sql, _, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "UPDATE test SET x = $1, y = $2"; sql != want {
		t.Errorf("expected %q, got %q instead", want, sql)
	}
}
