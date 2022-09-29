package pgq

// StatementBuilder builds SQL SELECT statements.
type StatementBuilder struct {
	whereParts []SQLizer
}

// Select returns a SelectBuilder for this StatementBuilder.
func (b StatementBuilder) Select(columns ...string) SelectBuilder {
	builder := SelectBuilder{}.Columns(columns...)
	builder.whereParts = b.whereParts
	return builder
}

// Insert returns a InsertBuilder for this StatementBuilder.
func (b StatementBuilder) Insert(into string) InsertBuilder {
	builder := InsertBuilder{}.Into(into)
	if b.whereParts != nil {
		builder.selectBuilder = &SelectBuilder{
			whereParts: b.whereParts,
		}
	}
	return builder
}

// Replace returns a InsertBuilder for this StatementBuilder with the
// statement keyword set to "REPLACE".
func (b StatementBuilder) Replace(into string) InsertBuilder {
	builder := b.Insert(into)
	builder.replace = true
	return builder
}

// Update returns a UpdateBuilder for this StatementBuilder.
func (b StatementBuilder) Update(table string) UpdateBuilder {
	builder := UpdateBuilder{}.Table(table)
	builder.whereParts = b.whereParts
	return builder
}

// Delete returns a DeleteBuilder for this StatementBuilder.
func (b StatementBuilder) Delete(from string) DeleteBuilder {
	builder := DeleteBuilder{}.From(from)
	builder.whereParts = b.whereParts
	return builder
}

// Where adds WHERE expressions to the query.
//
// See SelectBuilder.Where for more information.
func (b StatementBuilder) Where(pred any, args ...any) StatementBuilder {
	b.whereParts = []SQLizer{newWherePart(pred, args...)}
	return b
}

// Statement returns a new StatementBuilder, which can be used to create SQL WHERE parts.
func Statement() StatementBuilder {
	return StatementBuilder{}
}

// Select returns a new SelectBuilder, optionally setting some result columns.
//
// See SelectBuilder.Columns.
func Select(columns ...string) SelectBuilder {
	return SelectBuilder{}.Columns(columns...)
}

// Insert returns a new InsertBuilder with the given table name.
//
// See InsertBuilder.Into.
func Insert(into string) InsertBuilder {
	return InsertBuilder{into: into}
}

// Replace returns a new InsertBuilder with the statement keyword set to
// "REPLACE" and with the given table name.
//
// See InsertBuilder.Into.
func Replace(into string) InsertBuilder {
	builder := InsertBuilder{}
	builder.replace = true
	return builder.Into(into)
}

// Update returns a new UpdateBuilder with the given table name.
//
// See UpdateBuilder.Table.
func Update(table string) UpdateBuilder {
	return UpdateBuilder{table: table}
}

// Delete returns a new DeleteBuilder with the given table name.
//
// See DeleteBuilder.Table.
func Delete(from string) DeleteBuilder {
	return DeleteBuilder{from: from}
}

// Case returns a new CaseBuilder
// "what" represents case value
func Case(what ...any) CaseBuilder {
	b := CaseBuilder{}

	switch len(what) {
	case 0:
	case 1:
		b = b.what(what[0])
	default:
		b = b.what(newPart(what[0], what[1:]...))

	}
	return b
}
