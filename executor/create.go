package executor

import (
	store "github.com/polentadb/polenta-core-go/store"
)

type CreateExecutor struct {
	statement string
}

func (s CreateExecutor) Execute() Response {
	execute(s.statement)
	return Response{Message: "Executed create statement"}
}

func execute(statement string) {
	objectType := findObjectType(statement)
	objectName := findObjectName(statement)
	objectFields := findObjectFields(statement)
	store.Add(objectName, objectType, objectFields)
}

func findObjectName(_ string) string {
	return "TBD"
}

func findObjectType(_ string) string {
	return "bag"
}

func findObjectFields(_ string) map[string]string {
	var fields map[string]string
	return fields
}
