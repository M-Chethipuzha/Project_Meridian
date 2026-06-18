package almanac

import (
	"testing"
	"time"
)

func TestChangeEventKey(t *testing.T) {
	tests := []struct {
		name string
		evt  ChangeEvent
		want string
	}{
		{
			name: "simple wiki and title",
			evt: ChangeEvent{
				Wiki:  "enwiki",
				Title: "Albert Einstein",
			},
			want: "enwiki/Albert Einstein",
		},
		{
			name: "special characters",
			evt: ChangeEvent{
				Wiki:  "dewiki",
				Title: "Über_die_speziellen_und_allgemeinen_Relativitätstheorien",
			},
			want: "dewiki/Über_die_speziellen_und_allgemeinen_Relativitätstheorien",
		},
		{
			name: "empty fields",
			evt: ChangeEvent{
				Wiki:  "",
				Title: "",
			},
			want: "/",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.evt.Key()
			if got != tc.want {
				t.Errorf("Key() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestChangeEventParsedTimestamp(t *testing.T) {
	ts := int64(1718000000)
	expected := time.Unix(ts, 0).UTC()

	evt := ChangeEvent{
		Timestamp:       ts,
		ParsedTimestamp: expected,
	}

	if !evt.ParsedTimestamp.Equal(expected) {
		t.Errorf("ParsedTimestamp = %v, want %v", evt.ParsedTimestamp, expected)
	}
}
