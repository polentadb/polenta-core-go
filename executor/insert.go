package executor

type InsertExecutor struct {
	statement string
}

func (s InsertExecutor) Execute() Response {
	return Response{Message: "Executed insert statement"}
}
