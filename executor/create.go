package executor

import (
	store "github.com/polentadb/polenta-core-go/store"
	"strings"
)

type CreateExecutor struct {
	statement string
}

func (s CreateExecutor) Execute() Response {
	return Response{Message: execute(s.statement)}
}

func execute(statement string) string {
	objectType := findObjectType(statement)
	objectName := findObjectName(statement)
	if objectType == "BAG" || objectType == "TABLE" {
		objectFields := findObjectFields(statement)
		return store.AddCollection(objectName, objectType, objectFields)
	} else if objectType == "USER" {
		return store.AddObject(objectName, objectType)
	} else {
		return "Object type " + objectType + " not supported"
	}
}

func findObjectName(statement string) string {
	parts := strings.Split(strings.ToUpper(statement), " ")
	return strings.Trim(parts[2], " ")
}

func findObjectType(statement string) string {
	parts := strings.Split(strings.ToUpper(statement), " ")
	return strings.Trim(parts[1], " ")
}

func findObjectFields(_ string) map[string]string {
	var fields map[string]string
	return fields
}
