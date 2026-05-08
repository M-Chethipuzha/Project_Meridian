package almanac

import "testing"

func TestChangeEventKey(t *testing.T) {
	e := ChangeEvent{Wiki: "enwiki", Title: "Go_(programming_language)"}
	key := e.Key()
	if key != "enwiki/Go_(programming_language)" {
		t.Fatalf("unexpected key: %s", key)
	}
}

func TestChangeEventKeyWithSlash(t *testing.T) {
	e := ChangeEvent{Wiki: "commons", Title: "File:Test.jpg"}
	if e.Key() != "commons/File:Test.jpg" { t.Fatal("unexpected key") }
}
