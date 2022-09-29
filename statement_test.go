package pgq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStatementBuilderWhere(t *testing.T) {
	t.Parallel()
	sb := Statement().Where("x = ?", 1)

	sql, args, err := sb.Select("test").Where("y = ?", 2).SQL()
	assert.NoError(t, err)

	expectedSQL := "SELECT test WHERE x = $1 AND y = $2"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2}
	assert.Equal(t, expectedArgs, args)
}
