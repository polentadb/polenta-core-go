package executor

import (
	"testing"
)

func TestFindCollectionName(t *testing.T) {
	sql1 := "select * from person"
	collectionName1 := findCollectionName(sql1)
	if collectionName1 != "PERSON" {
		t.Error("Expected PERSON, got ", collectionName1)
	}
	sql2 := "select * from role where id = 1"
	collectionName2 := findCollectionName(sql2)
	if collectionName2 != "ROLE" {
		t.Error("Expected ROLE, got ", collectionName2)
	}
}
