package pgq

import (
	"reflect"
	"testing"
)

func TestInsertBuilderSQL(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		b        InsertBuilder
		wantSQL  string
		wantArgs []any
		wantErr  error
	}{
		{
			name: "with_suffix",
			b: Insert("").
				Prefix("WITH prefix AS ?", 0).
				Into("a").
				Columns("b", "c").
				Values(1, 2).
				Values(3, Expr("? + 1", 4)).
				Suffix("RETURNING ?", 5),
			wantSQL: "WITH prefix AS $1 " +
				"INSERT INTO a (b,c) VALUES ($2,$3),($4,$5 + 1) " +
				"RETURNING $6",
			wantArgs: []any{0, 1, 2, 3, 4, 5},
		},
		{
			name: "returning",
			b: Insert("a").
				Columns("b", "c").
				Values(1, 2).
				Values(3, Expr("? + 1", 4)).
				Returning("x"),
			wantSQL:  "INSERT INTO a (b,c) VALUES ($1,$2),($3,$4 + 1) RETURNING x",
			wantArgs: []any{1, 2, 3, 4},
		},
		{
			name: "returning_2",
			b: Insert("a").
				Columns("b", "c").
				Values(1, 2).
				Values(3, Expr("? + 1", 4)).
				Returning("x", "y"),
			wantSQL:  "INSERT INTO a (b,c) VALUES ($1,$2),($3,$4 + 1) RETURNING x, y",
			wantArgs: []any{1, 2, 3, 4},
		},
		{
			name: "returning_3",
			b: Insert("a").
				Columns("b", "c").
				Values(1, 2).
				Values(3, Expr("? + 1", 4)).
				Returning("x", "y", "z"),
			wantSQL:  "INSERT INTO a (b,c) VALUES ($1,$2),($3,$4 + 1) RETURNING x, y, z",
			wantArgs: []any{1, 2, 3, 4},
		},
		{
			name: "returning_3_multi_calls",
			b: Insert("a").
				Columns("b", "c").
				Values(1, 2).
				Values(3, Expr("? + 1", 4)).
				Returning("x", "y").Returning("z"),
			wantSQL:  "INSERT INTO a (b,c) VALUES ($1,$2),($3,$4 + 1) RETURNING x, y, z",
			wantArgs: []any{1, 2, 3, 4},
		},
		{
			name: "returning_select",
			b: Insert("a").
				Columns("b", "c").
				Values(1, 2).
				Values(3, Expr("? + 1", 4)).
				ReturningSelect(Select("abc").From("atable"), "something"),
			wantSQL:  "INSERT INTO a (b,c) VALUES ($1,$2),($3,$4 + 1) RETURNING (SELECT abc FROM atable) AS something",
			wantArgs: []any{1, 2, 3, 4},
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

func TestInsertStruct(t *testing.T) {
	example := struct{ What string }{What: "lol"}
	t.Parallel()
	b := Insert("").
		Into("a").
		Columns("something", "extra").
		Values(1, example)

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if want := "INSERT INTO a (something,extra) VALUES ($1,$2)"; want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1, example}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestInsertBuilderSQLErr(t *testing.T) {
	t.Parallel()
	_, _, err := Insert("").Values(1).SQL()
	if want := "insert statements must specify a table"; err.Error() != want {
		t.Errorf("expected error to be %q, got %q instead", want, err)
	}

	_, _, err = Insert("x").SQL()
	if want := "insert statements must have at least one set of values or select clause"; err.Error() != want {
		t.Errorf("expected error to be %q, got %q instead", want, err)
	}
}

func TestInsertBuilderMustSQL(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestInsertBuilderMustSQL should have panicked!")
		}
	}()
	Insert("").MustSQL()
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	t.Parallel()
	b := Insert("test").Values(1, 2)

	sql, _, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "INSERT INTO test VALUES ($1,$2)"; sql != want {
		t.Errorf("expected %q, got %q instead", sql, want)
	}
}

func TestInsertBuilderRunners(t *testing.T) {
	t.Parallel()
	b := Insert("test").Values(1)

	want := "INSERT INTO test VALUES ($1)"

	got, args := b.MustSQL()
	if want != got {
		t.Errorf("expected SQL to be %q, got %q instead", want, got)
	}
	if expectedArgs := []any{1}; !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("wanted %v, got %v instead", expectedArgs, args)
	}
}

func TestInsertBuilderSetMap(t *testing.T) {
	t.Parallel()
	b := Insert("table").SetMap(Eq{"field1": 1, "field2": 2, "field3": 3})

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "INSERT INTO table (field1,field2,field3) VALUES ($1,$2,$3)"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1, 2, 3}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestInsertBuilderSelect(t *testing.T) {
	t.Parallel()
	sb := Select("field1").From("table1").Where(Eq{"field1": 1})
	ib := Insert("table2").Columns("field1").Select(sb)

	sql, args, err := ib.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = $1"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestInsertBuilderReplace(t *testing.T) {
	t.Parallel()
	b := Replace("table").Values(1)

	want := "REPLACE INTO table VALUES ($1)"

	sql, _, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
}

func TestInsertBuilderVerb(t *testing.T) {
	t.Parallel()
	b := Insert("table").Verb("REPLACE").Values(1)

	want := "REPLACE INTO table VALUES ($1)"

	sql, _, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
}
