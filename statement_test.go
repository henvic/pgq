package pgq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatementBuilderWhere(t *testing.T) {
	t.Parallel()
	sb := StatementBuilder.Where("x = ?", 1)

	sql, args, err := sb.Select("test").Where("y = ?", 2).SQL()
	assert.NoError(t, err)

	expectedSql := "SELECT test WHERE x = ? AND y = ?"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{1, 2}
	assert.Equal(t, expectedArgs, args)
}
