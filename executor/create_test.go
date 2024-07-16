package executor

import (
	"testing"
)

func TestInsertionSort(t *testing.T) {
	objType := findObjectType("create bag person (name string, age int)")
	if objType != "BAG" {
		t.Error("Expected BAG, got", objType)
	}
	objName := findObjectName("create bag person (name string, age int)")
	if objName != "PERSON" {
		t.Error("Expected PERSON, got", objType)
	}
}
