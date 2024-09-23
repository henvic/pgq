# pgq
[![GoDoc](https://godoc.org/github.com/henvic/pgq?status.svg)](https://godoc.org/github.com/henvic/pgq) [![Build Status](https://github.com/henvic/pgq/workflows/Tests/badge.svg)](https://github.com/henvic/pgq/actions?query=workflow%3ATests) [![Coverage Status](https://coveralls.io/repos/henvic/pgq/badge.svg)](https://coveralls.io/r/henvic/pgq)

pgq is a query builder for PostgreSQL written in Go.

It is a fork of [Squirrel](https://github.com/Masterminds/squirrel) more suitable for working with the PostgreSQL database when you are able to use the native PostgreSQL protocol directly, rather than the slower textual protocol used by database/sql.

You can use it with the [pgx](https://github.com/jackc/pgx) driver.

## Usage example

Something like this:

```go
sql, args, err := pgq.Update("employees").
	Set("salary_bonus", pgq.Expr("salary_bonus + 1000")).
	From("accounts").
	Where("accounts.team = ?", "engineering").
	Returning("id", "name", "salary").SQL()

if err != nil {
	panic(err) // bug: this should never happen.
}

type employee struct {
	ID string
	Name string
	Salary int
}
var data []employee
rows, err := pool.Query(context.Background(), sql, args...)
if err == nil {
	defer rows.Close()
	data, err = pgx.CollectRows(rows, pgx.RowTo[employee])
}
```

## Main benefits

* API is crafted with only PostgreSQL compatibility so it has a somewhat lean API.
* It uses ANY and ALL [operators for slices](https://www.postgresql.org/docs/current/functions-comparisons.html) by default, which means it supports slices out of the box and you get to reuse your prepared statements.
* It's throughly tested (including integration tests to check for invalid queries being generated).
* If you already use pgx with Squirrel and the native PostgreSQL protocol, switching is very straightforward with just a few breaking changes (example: Alias is a type rather than a function).

## Main drawback

* It's still a query builder. You can go a long way writing pure SQL queries. Consider doing so.

## FAQ

**Why forking a query builder then?** Whatever it takes to make people ditch using an [ORM](https://alanilling.com/exiting-the-vietnam-of-programming-our-journey-in-dropping-the-orm-in-golang-3ce7dff24a0f), I guess.

## See also
* [pgtools](https://github.com/henvic/pgtools/)
* [pgxtutorial](https://github.com/henvic/pgxtutorial)
