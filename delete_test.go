package pgq

import (
	"reflect"
	"testing"
)

func TestDeleteBuilderSQL(t *testing.T) {
	t.Parallel()
	beginning := Delete("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Where("b = ?", 1).
		OrderBy("c")

	testCases := []struct {
		name     string
		b        DeleteBuilder
		wantSQL  string
		wantArgs []any
		wantErr  error
	}{
		{
			name: "with_suffix",
			b: Delete("").
				Prefix("WITH prefix AS ?", 0).
				From("a").
				Where("b = ?", 1).
				OrderBy("c").
				Suffix("RETURNING ?", 4),
			wantSQL:  "WITH prefix AS $1 DELETE FROM a WHERE b = $2 ORDER BY c RETURNING $3",
			wantArgs: []any{0, 1, 4},
		},
		{
			name:     "returning",
			b:        beginning.Returning("x"),
			wantSQL:  "WITH prefix AS $1 DELETE FROM a WHERE b = $2 ORDER BY c RETURNING x",
			wantArgs: []any{0, 1},
		},
		{
			name:     "returning_2",
			b:        beginning.Returning("x", "y"),
			wantSQL:  "WITH prefix AS $1 DELETE FROM a WHERE b = $2 ORDER BY c RETURNING x, y",
			wantArgs: []any{0, 1},
		},
		{
			name:     "returning_3",
			b:        beginning.Returning("x", "y", "z"),
			wantSQL:  "WITH prefix AS $1 DELETE FROM a WHERE b = $2 ORDER BY c RETURNING x, y, z",
			wantArgs: []any{0, 1},
		},
		{
			name:     "returning_3_multi_calls",
			b:        beginning.Returning("x", "y").Returning("z"),
			wantSQL:  "WITH prefix AS $1 DELETE FROM a WHERE b = $2 ORDER BY c RETURNING x, y, z",
			wantArgs: []any{0, 1},
		},
		{
			name:     "returning_select",
			b:        beginning.ReturningSelect(Select("abc").From("atable"), "something"),
			wantSQL:  "WITH prefix AS $1 DELETE FROM a WHERE b = $2 ORDER BY c RETURNING (SELECT abc FROM atable) AS something",
			wantArgs: []any{0, 1},
		},
		{
			name:     "delete_using",
			b:        Delete("films").Using("producers").Where("producer_id = producers.id").Where("producers.name = ?", "foo"),
			wantSQL:  "DELETE FROM films USING producers WHERE producer_id = producers.id AND producers.name = $1",
			wantArgs: []any{"foo"},
		},
		{
			name:     "delete_using_select",
			b:        Delete("films").UsingSelect(Select("id").From("producers").Where("name = ?", "foo"), "p"),
			wantSQL:  "DELETE FROM films USING (SELECT id FROM producers WHERE name = $1) AS p",
			wantArgs: []any{"foo"},
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
