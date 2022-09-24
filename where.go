package pgq

import (
	"fmt"
)

type wherePart part

func newWherePart(pred any, args ...any) SQLizer {
	return &wherePart{pred: pred, args: args}
}

func (p wherePart) SQL() (sql string, args []any, err error) {
	switch pred := p.pred.(type) {
	case nil:
		// no-op
	case rawSQLizer:
		return pred.unfinalizedSQL()
	case SQLizer:
		return pred.SQL()
	case map[string]any:
		return Eq(pred).SQL()
	case string:
		sql = pred
		args = p.args
	default:
		err = fmt.Errorf("expected string-keyed map or string, not %T", pred)
	}
	return
}
