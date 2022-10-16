package pgq

import (
	"reflect"
	"testing"
)

func TestDeleteBuilderSQL(t *testing.T) {
	t.Parallel()
	b := Delete("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Where("b = ?", 1).
		OrderBy("c").
		Suffix("RETURNING ?", 4)

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want :=
		"WITH prefix AS $1 " +
			"DELETE FROM a WHERE b = $2 ORDER BY c RETURNING $3"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{0, 1, 4}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestDeleteBuilderSQLErr(t *testing.T) {
	t.Parallel()
	_, _, err := Delete("").SQL()
	want := "delete statements must specify a From table"
	if err.Error() != want {
		t.Errorf("expected error to be %q, got %q instead", want, err)
	}
}

func TestDeleteBuilderMustSQL(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestDeleteBuilderMustSQL should have panicked!")
		}
	}()
	Delete("").MustSQL()
}

func TestDeleteBuilder(t *testing.T) {
	t.Parallel()
	b := Delete("test").Where("x = ? AND y = ?", 1, 2)

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	expectedArgs := []any{1, 2}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("wanted %v, got %v instead", expectedArgs, sql)
	}
	if want := "DELETE FROM test WHERE x = $1 AND y = $2"; sql != want {
		t.Errorf("expected %q, got %q instead", want, sql)
	}
}

func TestDeleteWithQuery(t *testing.T) {
	t.Parallel()
	b := Delete("test").Where("id=55").Suffix("RETURNING path")

	want := "DELETE FROM test WHERE id=55 RETURNING path"

	got, args := b.MustSQL()
	if got != want {
		t.Errorf("expected %q, got %q instead", want, got)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
}
