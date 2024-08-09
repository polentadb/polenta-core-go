package executor

import (
	"fmt"
	"github.com/polentadb/polenta-core-go/storage"
	"strings"
)

type InsertExecutor struct {
	statement string
}

func (s InsertExecutor) Execute() Response {
	collectionName := findInsertCollectionName(s.statement)
	if !storage.HasCollection(collectionName) {
		fmt.Println("ERROR: INSERT INTO INVALID COLLECTION: " + collectionName)
		return Response{Error: fmt.Sprintf("ERROR - INVALID INSERT - NO SUCH BAG OR TABLE: %s", collectionName)}
	}

	storage.AcquireCollectionWriteLock(collectionName)
	defer storage.ReleaseCollectionWriteLock(collectionName)

	if storage.HasSequenceColumn(collectionName) {
		sequenceNewValue := storage.NewSequenceValue(collectionName)
		fmt.Println("DEBUG: new sequence value = ", sequenceNewValue)
	}

	return Response{Message: "OK - EXECUTED INSERT STATEMENT. INTO: " + collectionName}
}

func findInsertCollectionName(sql string) string {
	upSql := strings.ToUpper(sql)
	firstParen := strings.Index(upSql, "(")
	return upSql[12 : firstParen-1]
}
