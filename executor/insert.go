package executor

import (
	"fmt"
	"github.com/polentadb/polenta-core-go/store"
	"strings"
)

type InsertExecutor struct {
	statement string
}

func (s InsertExecutor) Execute() Response {
	collectionName := findInsertCollectionName(s.statement)
	if !store.HasCollection(collectionName) {
		fmt.Println("ERROR: INSERT INTO INVALID COLLECTION: " + collectionName)
		return Response{Error: fmt.Sprintf("ERROR - INVALID INSERT - NO SUCH BAG OR TABLE: %s", collectionName)}
	}
	fmt.Println("DEBUG: INSERT INTO: " + collectionName)

	store.AcquireCollectionWriteLock(collectionName)
	defer store.ReleaseCollectionWriteLock(collectionName)

	colDef := store.GetCollection(collectionName)

	if hasSequenceColumn(colDef) {
		sequenceNewValue := store.NewSequenceValue(collectionName)
		fmt.Println(sequenceNewValue)
	}

	return Response{Message: "OK - EXECUTED INSERT STATEMENT. INTO: " + collectionName}
}

func hasSequenceColumn(_ store.CollectionDefinition) bool {
	return false
}

func findInsertCollectionName(sql string) string {
	upSql := strings.ToUpper(sql)
	firstParen := strings.Index(upSql, "(")
	return upSql[12 : firstParen-1]
}
