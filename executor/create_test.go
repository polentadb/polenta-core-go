package executor

import (
	"testing"
)

func TestCreateObjects(t *testing.T) {
	objType := findObjectType("create bag person (name string, age int)")
	if objType != "BAG" {
		t.Error("Expected BAG, got", objType)
	}
	objName1 := findObjectName("create bag person (name string, age int)")
	if objName1 != "PERSON" {
		t.Error("Expected PERSON, got", objName1)
	}
	objName2 := findObjectName("create user dba")
	if objName2 != "DBA" {
		t.Error("Expected DBA, got", objName2)
	}
	parts := parts("  id sequence , name string, age int,salary float( 10, 3), role_id int( 7 ), dob   date")
	if len(parts) != 6 {
		t.Error("Expected map with 6 elements, got", len(parts))
	}
	columns := findCollectionColumns("create bag person (  id sequence , name string, age int,salary float( 10, 3), role_id int( 7 ), dob   date)")
	if len(columns) != 6 {
		t.Error("Expected map with 6 elements, got", len(columns))
	}
	idDef, hasId := columns["ID"]
	if !hasId {
		t.Error("Expected column ID")
	}
	if idDef.Type != "SEQUENCE" {
		t.Error("Expected column ID of type SEQUENCE, got ", idDef.Type)
	}
	if idDef.Size != 0 {
		t.Error("Expected column ID of size 0, got ", idDef.Size)
	}
	if idDef.Precision != 0 {
		t.Error("Expected column ID of precision 0, got ", idDef.Precision)
	}
	nameDef, hasName := columns["NAME"]
	if !hasName {
		t.Error("Expected column NAME")
	}
	if nameDef.Type != "STRING" {
		t.Error("Expected column NAME of type STRING, got ", nameDef.Type)
	}
	if nameDef.Size != 0 {
		t.Error("Expected column NAME of size 0, got ", nameDef.Size)
	}
	if nameDef.Precision != 0 {
		t.Error("Expected column NAME of precision 0, got ", nameDef.Precision)
	}
	ageDef, hasAge := columns["AGE"]
	if !hasAge {
		t.Error("Expected column AGE")
	}
	if ageDef.Type != "INT" {
		t.Error("Expected column AGE of type INT, got ", ageDef.Type)
	}
	if ageDef.Size != 0 {
		t.Error("Expected column AGE of size 0, got ", ageDef.Size)
	}
	if ageDef.Precision != 0 {
		t.Error("Expected column AGE of precision 0, got ", nameDef.Precision)
	}
	salaryDef, hasSalary := columns["SALARY"]
	if !hasSalary {
		t.Error("Expected column SALARY")
	}
	if salaryDef.Type != "FLOAT" {
		t.Error("Expected column SALARY of type FLOAT(10,3), got ", salaryDef.Type)
	}
	if salaryDef.Size != 10 {
		t.Error("Expected column SALARY of size 10, got ", salaryDef.Size)
	}
	if salaryDef.Precision != 3 {
		t.Error("Expected column SALARY of precision 3, got ", salaryDef.Precision)
	}
	roleIdDef, hasRoleId := columns["ROLE_ID"]
	if !hasRoleId {
		t.Error("Expected column ROLE_ID")
	}
	if roleIdDef.Type != "INT" {
		t.Error("Expected column ROLE_ID of type INT, got ", roleIdDef.Type)
	}
	if roleIdDef.Size != 7 {
		t.Error("Expected column ROLE_ID of size 7, got ", roleIdDef.Size)
	}
	if roleIdDef.Precision != 0 {
		t.Error("Expected column ROLE_ID of precision 0, got ", roleIdDef.Precision)
	}
	dobDef, hasDob := columns["DOB"]
	if !hasDob {
		t.Error("Expected column DOB")
	}
	if dobDef.Type != "DATE" {
		t.Error("Expected column DOB of type DATE, got ", dobDef.Type)
	}
	if dobDef.Size != 0 {
		t.Error("Expected column DOB of size 0, got ", dobDef.Size)
	}
	if dobDef.Precision != 0 {
		t.Error("Expected column DOB of precision 0, got ", dobDef.Precision)
	}
}
