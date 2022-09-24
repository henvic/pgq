package pgq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCaseWithVal(t *testing.T) {
	t.Parallel()
	caseStmt := Case("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", "big number"))

	qb := Select().
		Column(caseStmt).
		From("table")
	sql, args, err := qb.SQL()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE number " +
		"WHEN 1 THEN one " +
		"WHEN 2 THEN two " +
		"ELSE ? " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{"big number"}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithComplexVal(t *testing.T) {
	t.Parallel()
	caseStmt := Case("? > ?", 10, 5).
		When("true", "'T'")

	qb := Select().
		Column(Alias(caseStmt, "complexCase")).
		From("table")
	sql, args, err := qb.SQL()

	assert.NoError(t, err)

	expectedSql := "SELECT (CASE ? > ? " +
		"WHEN true THEN 'T' " +
		"END) AS complexCase " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{10, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoVal(t *testing.T) {
	t.Parallel()
	caseStmt := Case().
		When(Eq{"x": 0}, "x is zero").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.SQL()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE " +
		"WHEN x = ? THEN x is zero " +
		"WHEN x > ? THEN CONCAT('x is greater than ', ?) " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{0, 1, 2}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithExpr(t *testing.T) {
	t.Parallel()
	caseStmt := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.SQL()

	assert.NoError(t, err)

	expectedSql := "SELECT CASE x = ? " +
		"WHEN true THEN ? " +
		"ELSE 42 " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{true, "it's true!"}
	assert.Equal(t, expectedArgs, args)
}

func TestMultipleCase(t *testing.T) {
	t.Parallel()
	caseStmtNoval := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")
	caseStmtExpr := Case().
		When(Eq{"x": 0}, "'x is zero'").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().
		Column(Alias(caseStmtNoval, "case_noval")).
		Column(Alias(caseStmtExpr, "case_expr")).
		From("table")

	sql, args, err := qb.SQL()

	assert.NoError(t, err)

	expectedSql := "SELECT " +
		"(CASE x = ? WHEN true THEN ? ELSE 42 END) AS case_noval, " +
		"(CASE WHEN x = ? THEN 'x is zero' WHEN x > ? THEN CONCAT('x is greater than ', ?) END) AS case_expr " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []any{
		true, "it's true!",
		0, 1, 2,
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoWhenClause(t *testing.T) {
	t.Parallel()
	caseStmt := Case("something").
		Else("42")

	qb := Select().Column(caseStmt).From("table")

	_, _, err := qb.SQL()

	assert.Error(t, err)

	assert.Equal(t, "case expression must contain at lease one WHEN clause", err.Error())
}

func TestCaseBuilderMustSql(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestCaseBuilderMustSql should have panicked!")
		}
	}()
	Case("").MustSQL()
}
