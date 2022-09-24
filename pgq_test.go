package pgq

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var sqlizer = Select("test")
var sqlStr = "SELECT test"

var testDebugUpdateSQL = Update("table").SetMap(Eq{"x": 1, "y": "val"})
var expectedDebugUpateSQL = "UPDATE table SET x = '1', y = 'val'"

func TestDebugSQLizerUpdateColon(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugUpateSQL, Debug(testDebugUpdateSQL))
}

func TestDebugSQLizerUpdateAtp(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugUpateSQL, Debug(testDebugUpdateSQL))
}

func TestDebugSQLizerUpdateDollar(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugUpateSQL, Debug(testDebugUpdateSQL))
}

func TestDebugSQLizerUpdateQuestion(t *testing.T) {
	testDebugUpdateSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugUpateSQL, Debug(testDebugUpdateSQL))
}

var testDebugDeleteSQL = Delete("table").Where(And{
	Eq{"column": "val"},
	Eq{"other": 1},
})
var expectedDebugDeleteSQL = "DELETE FROM table WHERE (column = 'val' AND other = '1')"

func TestDebugSQLizerDeleteColon(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugDeleteSQL, Debug(testDebugDeleteSQL))
}

func TestDebugSQLizerDeleteAtp(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugDeleteSQL, Debug(testDebugDeleteSQL))
}

func TestDebugSQLizerDeleteDollar(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugDeleteSQL, Debug(testDebugDeleteSQL))
}

func TestDebugSQLizerDeleteQuestion(t *testing.T) {
	testDebugDeleteSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugDeleteSQL, Debug(testDebugDeleteSQL))
}

var testDebugInsertSQL = Insert("table").Values(1, "test")
var expectedDebugInsertSQL = "INSERT INTO table VALUES ('1','test')"

func TestDebugSQLizerInsertColon(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugInsertSQL, Debug(testDebugInsertSQL))
}

func TestDebugSQLizerInsertAtp(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugInsertSQL, Debug(testDebugInsertSQL))
}

func TestDebugSQLizerInsertDollar(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugInsertSQL, Debug(testDebugInsertSQL))
}

func TestDebugSQLizerInsertQuestion(t *testing.T) {
	testDebugInsertSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugInsertSQL, Debug(testDebugInsertSQL))
}

var testDebugSelectSQL = Select("*").From("table").Where(And{
	Eq{"column": "val"},
	Eq{"other": 1},
})
var expectedDebugSelectSQL = "SELECT * FROM table WHERE (column = 'val' AND other = '1')"

func TestDebugSQLizerSelectColon(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(Colon)
	assert.Equal(t, expectedDebugSelectSQL, Debug(testDebugSelectSQL))
}

func TestDebugSQLizerSelectAtp(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(AtP)
	assert.Equal(t, expectedDebugSelectSQL, Debug(testDebugSelectSQL))
}

func TestDebugSQLizerSelectDollar(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(Dollar)
	assert.Equal(t, expectedDebugSelectSQL, Debug(testDebugSelectSQL))
}

func TestDebugSQLizerSelectQuestion(t *testing.T) {
	testDebugSelectSQL.PlaceholderFormat(Question)
	assert.Equal(t, expectedDebugSelectSQL, Debug(testDebugSelectSQL))
}

func TestDebug(t *testing.T) {
	sqlizer := Expr("x = ? AND y = ? AND z = '??'", 1, "text")
	expectedDebug := "x = '1' AND y = 'text' AND z = '?'"
	assert.Equal(t, expectedDebug, Debug(sqlizer))
}

func TestDebugSQLizerErrors(t *testing.T) {
	errorMsg := Debug(Expr("x = ?", 1, 2)) // Not enough placeholders
	assert.True(t, strings.HasPrefix(errorMsg, "[DebugSQLizer error: "))

	errorMsg = Debug(Expr("x = ? AND y = ?", 1)) // Too many placeholders
	assert.True(t, strings.HasPrefix(errorMsg, "[DebugSQLizer error: "))

	errorMsg = Debug(Lt{"x": nil}) // Cannot use nil values with Lt
	assert.True(t, strings.HasPrefix(errorMsg, "[SQL error: "))
}
