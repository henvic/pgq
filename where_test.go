package pgq

import (
	"reflect"
	"testing"

	"bytes"
)

func TestWherePartsAppendSQL(t *testing.T) {
	t.Parallel()
	parts := []SQLizer{
		newWherePart("x = ?", 1),
		newWherePart(nil),
		newWherePart(Eq{"y": 2}),
	}
	sql := &bytes.Buffer{}
	args, err := appendSQL(parts, sql, " AND ", []any{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want, got := "x = ? AND y = ?", sql.String(); want != got {
		t.Errorf("expected %q, got %q instead", want, got)
	}
	expectedArgs := []any{1, 2}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestWherePartsAppendSQLErr(t *testing.T) {
	t.Parallel()
	parts := []SQLizer{newWherePart(1)}
	_, err := appendSQL(parts, &bytes.Buffer{}, "", []any{})
	if want := "expected string-keyed map or string, not int"; err.Error() != want {
		t.Errorf("expected error to be %q, got %q instead", want, err)
	}
}

func TestWherePartNil(t *testing.T) {
	t.Parallel()
	sql, _, err := newWherePart(nil).SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if sql != "" {
		t.Errorf("expected WHERE to be empty")
	}
}

func TestWherePartErr(t *testing.T) {
	t.Parallel()
	_, _, err := newWherePart(1).SQL()

	if want := "expected string-keyed map or string, not int"; err.Error() != want {
		t.Errorf("expected error to be %q, got %q instead", want, err)
	}
}

func TestWherePartString(t *testing.T) {
	t.Parallel()
	sql, args, err := newWherePart("x = ?", 1).SQL()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "x = ?"; sql != want {
		t.Errorf("expected %q, got %q instead", want, sql)
	}
	expectedArgs := []any{1}
	if !reflect.DeepEqual(expectedArgs, args) {
		t.Errorf("wanted %v, got %v instead", args, expectedArgs)
	}
}

func TestWherePartMap(t *testing.T) {
	t.Parallel()
	test := func(pred any) {
		sql, _, err := newWherePart(pred).SQL()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		expect := []string{"x = ? AND y = ?", "y = ? AND x = ?"}
		if sql != expect[0] && sql != expect[1] {
			t.Errorf("expected one of %#v, got %#v", expect, sql)
		}
	}
	m := map[string]any{"x": 1, "y": 2}
	test(m)
	test(Eq(m))
}
