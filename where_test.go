package pgq

import (
	"testing"

	"bytes"

	"github.com/stretchr/testify/assert"
)

func TestWherePartsAppendSQL(t *testing.T) {
	parts := []SQLizer{
		newWherePart("x = ?", 1),
		newWherePart(nil),
		newWherePart(Eq{"y": 2}),
	}
	sql := &bytes.Buffer{}
	args, _ := appendSQL(parts, sql, " AND ", []any{})
	assert.Equal(t, "x = ? AND y = ?", sql.String())
	assert.Equal(t, []any{1, 2}, args)
}

func TestWherePartsAppendSQLErr(t *testing.T) {
	parts := []SQLizer{newWherePart(1)}
	_, err := appendSQL(parts, &bytes.Buffer{}, "", []any{})
	assert.Error(t, err)
}

func TestWherePartNil(t *testing.T) {
	sql, _, _ := newWherePart(nil).SQL()
	assert.Equal(t, "", sql)
}

func TestWherePartErr(t *testing.T) {
	_, _, err := newWherePart(1).SQL()
	assert.Error(t, err)
}

func TestWherePartString(t *testing.T) {
	sql, args, _ := newWherePart("x = ?", 1).SQL()
	assert.Equal(t, "x = ?", sql)
	assert.Equal(t, []any{1}, args)
}

func TestWherePartMap(t *testing.T) {
	test := func(pred any) {
		sql, _, _ := newWherePart(pred).SQL()
		expect := []string{"x = ? AND y = ?", "y = ? AND x = ?"}
		if sql != expect[0] && sql != expect[1] {
			t.Errorf("expected one of %#v, got %#v", expect, sql)
		}
	}
	m := map[string]any{"x": 1, "y": 2}
	test(m)
	test(Eq(m))
}
