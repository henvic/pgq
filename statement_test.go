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

func TestStatementBuilderUpdate(t *testing.T) {
	t.Parallel()
	b := Statement().Where("x = ?", "z").Update("foo").Set("a", "b")
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
	b := Statement().Where("x = ?", "z").Delete("foo")
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
