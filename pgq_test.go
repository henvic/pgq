package pgq

import (
	"testing"
)

func TestDebug(t *testing.T) {
	t.Parallel()
	sqlizer := Expr("x = ? AND y = ? AND z = '??'", 1, "text")
	expectedDebug := "x = '1' AND y = 'text' AND z = '?'"
	if got := Debug(sqlizer); got != expectedDebug {
		t.Errorf("expected %q, got %q instead", got, expectedDebug)
	}
}

func TestDebugSQLizerErrors(t *testing.T) {
	t.Parallel()
	var errorMessages = []struct {
		s    SQLizer
		want string
	}{
		// Not enough placeholders
		{
			s:    Expr("x = ?", 1, 2),
			want: "[DebugSQLizer error: not enough placeholders in \"\" for 2 args]",
		},
		// Too many placeholders
		{
			s:    Expr("x = ? AND y = ?", 1),
			want: "[DebugSQLizer error: too many placeholders in \" AND y = ?\" for 1 args]",
		},
		// Cannot use nil values with Lt
		{
			s:    Lt{"x": nil},
			want: "[SQL error: cannot use null with less than or greater than operators]",
		},
	}

	for _, m := range errorMessages {
		if msg := Debug(m.s); msg != m.want {
			t.Errorf("expected %q, got error message = %q instead", m.want, msg)
		}
	}
}
