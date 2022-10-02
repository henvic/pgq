package pgq

import (
	"strings"
	"testing"
)

func TestDollar(t *testing.T) {
	t.Parallel()
	sql := "x = ? AND y = ?"
	s, err := dollarPlaceholder(sql)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if want := "x = $1 AND y = $2"; s != want {
		t.Errorf("expected %q, got %q instead", want, s)
	}
}

func TestPlaceholders(t *testing.T) {
	t.Parallel()
	got := Placeholders(2)
	if want := "?,?"; got != want {
		t.Errorf("expected %q, got %q instead", want, got)
	}
}

func TestEscapeDollar(t *testing.T) {
	t.Parallel()
	sql := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ??| array['?'] AND enabled = ?"
	s, err := dollarPlaceholder(sql)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	want := "SELECT uuid, \"data\" #> '{tags}' AS tags FROM nodes WHERE  \"data\" -> 'tags' ?| array['$1'] AND enabled = $2"
	if s != want {
		t.Errorf("expected %q, got %q instead", want, s)
	}
}

func BenchmarkPlaceholdersArray(b *testing.B) {
	var count = b.N
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = "?"
	}
	var _ = strings.Join(placeholders, ",")
}

func BenchmarkPlaceholdersStrings(b *testing.B) {
	Placeholders(b.N)
}
