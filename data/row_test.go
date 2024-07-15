package data

import (
	"testing"
)

func TestCompareRows(t *testing.T) {

	r1 := Row{"name": NewStringValue("John"), "age": NewIntegerValue(25)}
	r2 := Row{"name": NewStringValue("John"), "age": NewIntegerValue(20)}
	int := CompareRows(r1, r2, "age")
	if int != 1 {
		t.Error("Expected 0, got ", int)
	}

}
