package executor

import (
	"testing"
)

func TestFindInsertCollectionName(t *testing.T) {
	sql1 := "insert into person (name, age) values (\"John\", 30)"
	collectionName1 := findInsertCollectionName(sql1)
	if collectionName1 != "PERSON" {
		t.Error("Expected PERSON, got ", collectionName1)
	}
	sql2 := "insert into employee (\"John\", 1000)"
	collectionName2 := findInsertCollectionName(sql2)
	if collectionName2 != "EMPLOYEE" {
		t.Error("Expected EMPLOYEE, got ", collectionName2)
	}
}
