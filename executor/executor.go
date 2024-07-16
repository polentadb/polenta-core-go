package executor

import (
	"strings"
)

type Response struct {
	ErrorCode int
	Error     string
	Message   string
	Body      string
}

type Executor interface {
	Execute() Response
}

func Create(statement string) (Executor, *int, *string) {
	if strings.HasPrefix(strings.ToUpper(statement), "CREATE") {
		return CreateExecutor{statement: statement}, nil, nil
	}
	if strings.HasPrefix(strings.ToUpper(statement), "SELECT") {
		return SelectExecutor{statement: statement}, nil, nil
	}
	if strings.HasPrefix(strings.ToUpper(statement), "INSERT") {
		return InsertExecutor{statement: statement}, nil, nil
	}
	errcode := 1
	err := "Invalid statement"
	return nil, &errcode, &err
}
