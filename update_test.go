package pgq

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, err)

	expectedSQL :=
		"WITH prefix AS ? " +
			"UPDATE a SET b = ? + 1, c = ?, " +
			"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
			"c2 = CASE WHEN a = 2 THEN ? WHEN a = 3 THEN ? END, " +
			"c3 = (SELECT a FROM b) " +
			"WHERE d = ? " +
			"ORDER BY e LIMIT 4 OFFSET 5 " +
			"RETURNING ?"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 1, 2, "foo", "bar", 3, 6}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderSQLErr(t *testing.T) {
	t.Parallel()
	_, _, err := Update("").Set("x", 1).SQL()
	assert.Error(t, err)

	_, _, err = Update("x").SQL()
	assert.Error(t, err)
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

	sql, _, _ := b.PlaceholderFormat(Question).SQL()
	assert.Equal(t, "UPDATE test SET x = ?, y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).SQL()
	assert.Equal(t, "UPDATE test SET x = $1, y = $2", sql)
}
