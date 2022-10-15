package pgq

import (
	"reflect"
	"testing"
)

func TestCaseWithVal(t *testing.T) {
	t.Parallel()
	caseStmt := Case("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", "big number"))

	qb := Select().
		Column(caseStmt).
		From("atable")
	sql, args, err := qb.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT CASE number " +
		"WHEN 1 THEN one " +
		"WHEN 2 THEN two " +
		"ELSE $1 " +
		"END " +
		"FROM atable"
	if sql != want {
		t.Errorf("wanted %v, got %v instead", want, sql)
	}

	expectedArgs := []any{"big number"}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("wanted %v, got %v instead", expectedArgs, sql)
	}
}

func TestCaseWithComplexVal(t *testing.T) {
	t.Parallel()
	caseStmt := Case("? > ?", 10, 5).
		When("true", "'T'")

	qb := Select().
		Column(Alias{
			Expr: caseStmt,
			As:   "complexCase",
		}).
		From("atable")
	sql, args, err := qb.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT (CASE $1 > $2 " +
		"WHEN true THEN 'T' " +
		"END) AS complexCase " +
		"FROM atable"
	if sql != want {
		t.Errorf("wanted %v, got %v instead", want, sql)
	}

	expectedArgs := []any{10, 5}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("wanted %v, got %v instead", expectedArgs, args)
	}
}

func TestCaseWithNoVal(t *testing.T) {
	t.Parallel()
	caseStmt := Case().
		When(Eq{"x": 0}, "x is zero").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().Column(caseStmt).From("atable")
	sql, args, err := qb.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT CASE " +
		"WHEN x = $1 THEN x is zero " +
		"WHEN x > $2 THEN CONCAT('x is greater than ', $3) " +
		"END " +
		"FROM atable"

	if sql != want {
		t.Errorf("wanted %v, got %v instead", want, sql)
	}

	expectedArgs := []any{0, 1, 2}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("wanted %v, got %v instead", expectedArgs, args)
	}
}

func TestCaseWithExpr(t *testing.T) {
	t.Parallel()
	caseStmt := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")

	qb := Select().Column(caseStmt).From("atable")
	sql, args, err := qb.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT CASE x = $1 " +
		"WHEN true THEN $2 " +
		"ELSE 42 " +
		"END " +
		"FROM atable"

	if sql != want {
		t.Errorf("wanted %v, got %v instead", want, sql)
	}

	expectedArgs := []any{true, "it's true!"}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("wanted %v, got %v instead", expectedArgs, args)
	}
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
		Column(Alias{
			Expr: caseStmtNoval,
			As:   "case_noval",
		}).
		Column(Alias{
			Expr: caseStmtExpr,
			As:   "case_expr",
		}).
		From("atable")

	sql, args, err := qb.SQL()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	want := "SELECT " +
		"(CASE x = $1 WHEN true THEN $2 ELSE 42 END) AS case_noval, " +
		"(CASE WHEN x = $3 THEN 'x is zero' WHEN x > $4 THEN CONCAT('x is greater than ', $5) END) AS case_expr " +
		"FROM atable"

	if sql != want {
		t.Errorf("wanted %v, got %v instead", want, sql)
	}

	expectedArgs := []any{
		true, "it's true!",
		0, 1, 2,
	}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Errorf("wanted %v, got %v instead", expectedArgs, args)
	}
}

func TestCaseWithNoWhenClause(t *testing.T) {
	t.Parallel()
	caseStmt := Case("something").
		Else("42")

	qb := Select().Column(caseStmt).From("atable")

	_, _, err := qb.SQL()

	want := "case expression must contain at lease one WHEN clause"
	if err.Error() != want {
		t.Errorf("wanted error to be %v, got %v instead", want, err)
	}
}

func TestCaseBuilderMustSQL(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestCaseBuilderMustSQL should have panicked!")
		}
	}()
	Case("").MustSQL()
}
