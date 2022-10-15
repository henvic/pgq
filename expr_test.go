package pgq

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestConcatExpr(t *testing.T) {
	t.Parallel()
	sql, args, err := ConcatSQL("COALESCE(name,", Expr("CONCAT(?,' ',?)", "f", "l"), ")")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "COALESCE(name,CONCAT(?,' ',?))"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{"f", "l"}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestConcatExprBadType(t *testing.T) {
	t.Parallel()
	_, _, err := ConcatSQL("prefix", 123, "suffix")
	want := "123 is not a string or SQLizer"
	if err.Error() != want {
		t.Errorf("expected error to be %q, got %q instead", want, err)
	}
	if want := "123 is not a string or SQLizer"; err.Error() != want {
		t.Errorf("expected %q, got %q instead", want, err)
	}
}

func TestEqSQL(t *testing.T) {
	t.Parallel()
	b := Eq{"id": 1}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "id = ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestEqEmptySQL(t *testing.T) {
	t.Parallel()
	sql, args, err := Eq{}.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "(TRUE)"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
}

func TestEqInSQL(t *testing.T) {
	t.Parallel()
	b := Eq{"id": []int{1, 2, 3}}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "id = ANY (?)"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{[]int{1, 2, 3}}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestNotEqSQL(t *testing.T) {
	t.Parallel()
	b := NotEq{"id": 1}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "id <> ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestEqNotInSQL(t *testing.T) {
	t.Parallel()
	b := NotEq{"id": []int{1, 2, 3}}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "id <> ALL (?)"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{[]int{1, 2, 3}}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestEqInEmptySQL(t *testing.T) {
	t.Parallel()
	b := Eq{"id": []int{}}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "(FALSE)"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestNotEqInEmptySQL(t *testing.T) {
	t.Parallel()
	b := NotEq{"id": []int{}}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "(TRUE)"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestEqBytesSQL(t *testing.T) {
	t.Parallel()
	b := Eq{"id": []byte("test")}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "id = ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{[]byte("test")}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestLtSQL(t *testing.T) {
	t.Parallel()
	b := Lt{"id": 1}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "id < ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestLtOrEqSQL(t *testing.T) {
	t.Parallel()
	b := LtOrEq{"id": 1}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "id <= ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestGtSQL(t *testing.T) {
	t.Parallel()
	b := Gt{"id": 1}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "id > ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestGtOrEqSQL(t *testing.T) {
	t.Parallel()
	b := GtOrEq{"id": 1}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "id >= ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestExprNilSQL(t *testing.T) {
	t.Parallel()
	var b SQLizer
	b = NotEq{"name": nil}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}

	want := "name IS NOT NULL"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	b = Eq{"name": nil}
	sql, args, err = b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}

	want = "name IS NULL"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}
}

func TestNullTypeString(t *testing.T) {
	t.Parallel()
	var b SQLizer
	var name sql.NullString

	b = Eq{"name": name}
	sql, args, err := b.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
	if want := "name IS NULL"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	name.Scan("Name")
	b = Eq{"name": name}
	sql, args, err = b.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := []any{"Name"}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected args to be %q, got %q instead", want, args)
	}
	if want := "name = ?"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}
}

func TestNullTypeInt64(t *testing.T) {
	t.Parallel()
	var userID sql.NullInt64
	userID.Scan(nil)
	b := Eq{"user_id": userID}
	sql, args, err := b.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
	if want := "user_id IS NULL"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	userID.Scan(int64(10))
	b = Eq{"user_id": userID}
	sql, args, err = b.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := []any{int64(10)}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected args to be %q, got %q instead", want, args)
	}
	if want := "user_id = ?"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}
}

func TestNilPointer(t *testing.T) {
	t.Parallel()
	var name *string = nil
	eq := Eq{"name": name}
	sql, args, err := eq.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
	if want := "name IS NULL"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	neq := NotEq{"name": name}
	sql, args, err = neq.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
	if want := "name IS NOT NULL"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	var ids *[]int = nil
	eq = Eq{"id": ids}
	sql, args, err = eq.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
	if want := "id IS NULL"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	neq = NotEq{"id": ids}
	sql, args, err = neq.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
	if want := "id IS NOT NULL"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	var ida *[3]int = nil
	eq = Eq{"id": ida}
	sql, args, err = eq.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
	if want := "id IS NULL"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	neq = NotEq{"id": ida}
	sql, args, err = neq.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(args) != 0 {
		t.Errorf("wanted 0 arguments, got %d instead", len(args))
	}
	if want := "id IS NOT NULL"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

}

func TestNotNilPointer(t *testing.T) {
	t.Parallel()
	c := "Name"
	name := &c
	eq := Eq{"name": name}
	sql, args, err := eq.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := []any{"Name"}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected args to be %q, got %q instead", want, args)
	}
	if want := "name = ?"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	neq := NotEq{"name": name}
	sql, args, err = neq.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := []any{"Name"}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected args to be %q, got %q instead", want, args)
	}
	if want := "name <> ?"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	s := []int{1, 2, 3}
	ids := &s
	eq = Eq{"id": ids}
	sql, args, err = eq.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := []any{[]int{1, 2, 3}}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected args to be %q, got %q instead", want, args)
	}
	if want := "id = ANY (?)"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	neq = NotEq{"id": ids}
	sql, args, err = neq.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := []any{[]int{1, 2, 3}}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected args to be %q, got %q instead", want, args)
	}
	if want := "id <> ALL (?)"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	a := [3]int{1, 2, 3}
	ida := &a
	eq = Eq{"id": ida}
	sql, args, err = eq.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := []any{[3]int{1, 2, 3}}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected args to be %q, got %q instead", want, args)
	}
	if want := "id = ANY (?)"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}

	neq = NotEq{"id": ida}
	sql, args, err = neq.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := []any{[3]int{1, 2, 3}}; !reflect.DeepEqual(args, want) {
		t.Errorf("expected args to be %q, got %q instead", want, args)
	}
	if want := "id <> ALL (?)"; sql != want {
		t.Errorf("expected args to be %q, got %q instead", want, sql)
	}
}

func TestEmptyAndSQL(t *testing.T) {
	t.Parallel()
	sql, args, err := And{}.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "(TRUE)"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestEmptyOrSQL(t *testing.T) {
	t.Parallel()
	sql, args, err := Or{}.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "(FALSE)"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestLikeSQL(t *testing.T) {
	t.Parallel()
	b := Like{"name": "%irrel"}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "name LIKE ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{"%irrel"}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestNotLikeSQL(t *testing.T) {
	t.Parallel()
	b := NotLike{"name": "%irrel"}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "name NOT LIKE ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{"%irrel"}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestILikeSQL(t *testing.T) {
	t.Parallel()
	b := ILike{"name": "sq%"}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "name ILIKE ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{"sq%"}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestNotILikeSQL(t *testing.T) {
	t.Parallel()
	b := NotILike{"name": "sq%"}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "name NOT ILIKE ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{"sq%"}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestSQLEqOrder(t *testing.T) {
	t.Parallel()
	b := Eq{"a": 1, "b": 2, "c": 3}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "a = ? AND b = ? AND c = ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1, 2, 3}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestSQLLtOrder(t *testing.T) {
	t.Parallel()
	b := Lt{"a": 1, "b": 2, "c": 3}
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "a < ? AND b < ? AND c < ?"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{1, 2, 3}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestExprEscaped(t *testing.T) {
	t.Parallel()
	b := Expr("count(??)", Expr("x"))
	sql, args, err := b.SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "count(??)"
	if want != sql {
		t.Errorf("expected SQL to be %q, got %q instead", want, sql)
	}

	expectedArgs := []any{Expr("x")}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestExprRecursion(t *testing.T) {
	t.Parallel()
	{
		b := Expr("count(?)", Expr("nullif(a,?)", "b"))
		sql, args, err := b.SQL()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		want := "count(nullif(a,?))"
		if want != sql {
			t.Errorf("expected SQL to be %q, got %q instead", want, sql)
		}

		expectedArgs := []any{"b"}
		if !reflect.DeepEqual(expectedArgs, args) {
			t.Errorf("wanted %v, got %v instead", args, expectedArgs)
		}
	}
	{
		b := Expr("extract(? from ?)", Expr("epoch"), "2001-02-03")
		sql, args, err := b.SQL()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		want := "extract(epoch from ?)"
		if want != sql {
			t.Errorf("expected SQL to be %q, got %q instead", want, sql)
		}

		expectedArgs := []any{"2001-02-03"}
		if !reflect.DeepEqual(expectedArgs, args) {
			t.Errorf("wanted %v, got %v instead", args, expectedArgs)
		}
	}
	{
		b := Expr("JOIN t1 ON ?", And{Eq{"id": 1}, Expr("NOT c1"), Expr("? @@ ?", "x", "y")})
		sql, args, err := b.SQL()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		want := "JOIN t1 ON (id = ? AND NOT c1 AND ? @@ ?)"
		if want != sql {
			t.Errorf("expected SQL to be %q, got %q instead", want, sql)
		}

		expectedArgs := []any{1, "x", "y"}
		if !reflect.DeepEqual(expectedArgs, args) {
			t.Errorf("wanted %v, got %v instead", args, expectedArgs)
		}
	}
}

func ExampleEq() {
	Select("id", "created", "first_name").From("users").Where(Eq{
		"company": 20,
	})
}
