package pgq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsertBuilderSQL(t *testing.T) {
	t.Parallel()
	b := Insert("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSQL :=
		"WITH prefix AS $1 " +
			"INSERT DELAYED IGNORE INTO a (b,c) VALUES ($2,$3),($4,$5 + 1) " +
			"RETURNING $6"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{0, 1, 2, 3, 4, 5}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderSQLErr(t *testing.T) {
	t.Parallel()
	_, _, err := Insert("").Values(1).SQL()
	assert.Error(t, err)

	_, _, err = Insert("x").SQL()
	assert.Error(t, err)
}

func TestInsertBuilderMustSQL(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("TestInsertBuilderMustSQL should have panicked!")
		}
	}()
	Insert("").MustSQL()
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	t.Parallel()
	b := Insert("test").Values(1, 2)

	sql, _, _ := b.SQL()
	assert.Equal(t, "INSERT INTO test VALUES ($1,$2)", sql)
}

func TestInsertBuilderRunners(t *testing.T) {
	t.Parallel()
	b := Insert("test").Values(1)

	expectedSQL := "INSERT INTO test VALUES ($1)"

	got, args := b.MustSQL()
	assert.Equal(t, expectedSQL, got)
	assert.Len(t, args, 1)
}

func TestInsertBuilderSetMap(t *testing.T) {
	t.Parallel()
	b := Insert("table").SetMap(Eq{"field1": 1, "field2": 2, "field3": 3})

	sql, args, err := b.SQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table (field1,field2,field3) VALUES ($1,$2,$3)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1, 2, 3}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderSelect(t *testing.T) {
	t.Parallel()
	sb := Select("field1").From("table1").Where(Eq{"field1": 1})
	ib := Insert("table2").Columns("field1").Select(sb)

	sql, args, err := ib.SQL()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = $1"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{1}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReplace(t *testing.T) {
	t.Parallel()
	b := Replace("table").Values(1)

	expectedSQL := "REPLACE INTO table VALUES ($1)"

	sql, _, err := b.SQL()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
}
