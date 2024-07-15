package data

import (
	"testing"
)

func TestCompareRows(t *testing.T) {

	r1 := Row{"name": NewStringValue("John"), "age": NewIntegerValue(25)}
	r2 := Row{"name": NewStringValue("John"), "age": NewIntegerValue(20)}
	compared := CompareRows(r1, r2, "age")
	if compared != 1 {
		t.Error("Expected 0, got ", compared)
	}

}
