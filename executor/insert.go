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
	collection := findInsertCollectionName(s.statement)
	if !store.HasCollection(collection) {
		fmt.Println("ERROR: INSERT INTO INVALID COLLECTION: " + collection)
		return Response{Error: fmt.Sprintf("ERROR - INVALID INSERT - NO SUCH BAG OR TABLE: %s", collection)}
	}
	fmt.Println("DEBUG: INSERT INTO: " + collection)

	store.AcquireCollectionWriteLock(collection)
	defer store.ReleaseCollectionWriteLock(collection)

	return Response{Message: "OK - EXECUTED INSERT STATEMENT. INTO: " + collection}
}

func findInsertCollectionName(sql string) string {
	upSql := strings.ToUpper(sql)
	firstParen := strings.Index(upSql, "(")
	return upSql[12 : firstParen-1]
}
