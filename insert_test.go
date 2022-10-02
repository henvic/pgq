package pgq

import (
	"reflect"
	"testing"
)

func TestInsertBuilderSQL(t *testing.T) {
	t.Parallel()
	b := Insert("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want :=
		"WITH prefix AS $1 " +
			"INSERT DELAYED IGNORE INTO a (b,c) VALUES ($2,$3),($4,$5 + 1) " +
			"RETURNING $6"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{0, 1, 2, 3, 4, 5}
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
