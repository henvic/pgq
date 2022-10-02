package pgq

import (
	"reflect"
	"testing"
)

func TestStatementBuilderWhere(t *testing.T) {
	t.Parallel()
	sb := Statement().Where("x = ?", 1)

	sql, args, err := sb.Select("test").Where("y = ?", 2).SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT test WHERE x = $1 AND y = $2"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1, 2}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestStatementBuilderInsert(t *testing.T) {
	t.Parallel()
	b := Statement().Insert("foo").Columns("x").Values(3)
	want := "INSERT INTO foo (x) VALUES ($1)"
	sql, args, err := b.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if sql != want {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	expectedArgs := []any{3}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("expected arguments to be %q, got %q instead", expectedArgs, args)
	}
}

func TestStatementBuilderWhereInsertError(t *testing.T) {
	t.Parallel()

	sb := Statement().Where(Eq{"field1": 1})
	ib := sb.Insert("table2").Columns("field1")

	_, _, err := ib.SQL()
	want := "select statements must have at least one result column"
	if err.Error() != want {
		t.Errorf("expected error to be %v, got %v instead", want, err)
	}
}

func TestStatementBuilderReplace(t *testing.T) {
	t.Parallel()
	b := Statement().Replace("foo").Columns("x").Values(3)
	want := "REPLACE INTO foo (x) VALUES ($1)"
	sql, args, err := b.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if sql != want {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	expectedArgs := []any{3}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("expected arguments to be %q, got %q instead", expectedArgs, args)
	}
}

func TestStatementBuilderUpdate(t *testing.T) {
	t.Parallel()
	b := Statement().Update("foo").Where("x = ?", "z").Set("a", "b")
	want := "UPDATE foo SET a = $1 WHERE x = $2"
	sql, args, err := b.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if sql != want {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	expectedArgs := []any{"b", "z"}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("expected arguments to be %q, got %q instead", expectedArgs, args)
	}
}

func TestStatementBuilderDelete(t *testing.T) {
	t.Parallel()
	b := Statement().Delete("foo").Where("x = ?", "z")
	want := "DELETE FROM foo WHERE x = $1"
	sql, args, err := b.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if sql != want {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	expectedArgs := []any{"z"}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("expected arguments to be %q, got %q instead", expectedArgs, args)
	}
}
