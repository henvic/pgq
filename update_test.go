package pgq

import (
	"reflect"
	"testing"
)

func TestUpdateBuilderSQL(t *testing.T) {
	t.Parallel()
	b := Update("").
		Prefix("WITH prefix AS ?", 0).
		Table("a").
		Set("b", Expr("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Set("c1", Case("status").When("1", "2").When("2", "1")).
		Set("c2", Case().When("a = 2", Expr("?", "foo")).When("a = 3", Expr("?", "bar"))).
		Set("c3", Select("a").From("b")).
		Where("d = ?", 3).
		OrderBy("e").
		Limit(4).
		Offset(5).
		Suffix("RETURNING ?", 6)

	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want :=
		"WITH prefix AS $1 " +
			"UPDATE a SET b = $2 + 1, c = $3, " +
			"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
			"c2 = CASE WHEN a = 2 THEN $4 WHEN a = 3 THEN $5 END, " +
			"c3 = (SELECT a FROM b) " +
			"WHERE d = $6 " +
			"ORDER BY e LIMIT 4 OFFSET 5 " +
			"RETURNING $7"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{0, 1, 2, "foo", "bar", 3, 6}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
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
