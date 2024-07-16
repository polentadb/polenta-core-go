package polenta

import executor "github.com/polentadb/polenta-core-go/executor"

type Response executor.Response

func Run(statement string) Response {
	exec, errCode, err := executor.Create(statement)
	if err != nil && errCode != nil {
		return CreateErrorResponse(*errCode, *err)
	}
	if exec != nil {
		return Response(exec.Execute())
	}
	return CreateErrorResponse(2, "Internal error")
}

func CreateErrorResponse(errCode int, err string) Response {
	return Response{ErrorCode: errCode, Error: err}
}
