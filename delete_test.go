package pgq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeleteBuilderSQL(t *testing.T) {
	t.Parallel()
	b := Delete("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Where("b = ?", 1).
		OrderBy("c").
		Limit(2).
		Offset(3).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSql :=
		"WITH prefix AS ? " +
			"DELETE FROM a WHERE b = ? ORDER BY c LIMIT 2 OFFSET 3 " +
			"RETURNING ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{0, 1, 4}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderSQLErr(t *testing.T) {
	t.Parallel()
	_, _, err := Delete("").SQL()
	assert.Error(t, err)
}

func TestDeleteBuilderMustSql(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestDeleteBuilderMustSql should have panicked!")
		}
	}()
	Delete("").MustSQL()
}

func TestDeleteBuilderPlaceholders(t *testing.T) {
	t.Parallel()
	b := Delete("test").Where("x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).SQL()
	assert.Equal(t, "DELETE FROM test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).SQL()
	assert.Equal(t, "DELETE FROM test WHERE x = $1 AND y = $2", sql)
}

func TestDeleteWithQuery(t *testing.T) {
	t.Parallel()
	b := Delete("test").Where("id=55").Suffix("RETURNING path")

	expectedSql := "DELETE FROM test WHERE id=55 RETURNING path"

	got, args := b.MustSQL()
	assert.Equal(t, expectedSql, got)
	assert.Empty(t, args)
}
