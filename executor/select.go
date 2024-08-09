package executor

import (
	"fmt"
	"github.com/polentadb/polenta-core-go/data"
	"github.com/polentadb/polenta-core-go/sorter"
	"github.com/polentadb/polenta-core-go/storage"
	"strconv"
	"strings"
)

type SelectExecutor struct {
	statement string
}

func (s SelectExecutor) Execute() Response {
	collection := findSelectCollectionName(s.statement)
	if !storage.HasCollection(collection) {
		fmt.Println("ERROR: SELECT FROM INVALID COLLECTION: " + collection)
		return Response{Error: fmt.Sprintf("ERROR - INVALID SELECT - NO SUCH BAG OR TABLE: %s", collection)}
	}
	//fmt.Println("DEBUG: SELECT FROM: " + collection)

	storage.AcquireCollectionReadLock(collection)
	defer storage.ReleaseCollectionReadLock(collection)

	fields := "TBD"
	where := "TBD"
	orderBy := "TBD"
	selected := selectFrom(collection, fields, where)
	sorted := sort(selected, orderBy)
	resultSet := resultSet(sorted)

	return Response{Message: "OK - EXECUTED SELECT STATEMENT. " + collection + ": SELECTED " + strconv.Itoa(resultSet.Statistics.Count) + " ROWS."}
}

func findSelectCollectionName(sql string) string {
	upSql := strings.ToUpper(sql)
	fromPos := strings.Index(upSql, "FROM")
	wherePos := strings.Index(upSql, "WHERE")
	if wherePos == -1 {
		return upSql[fromPos+5:]
	} else {
		return upSql[fromPos+5 : wherePos-1]
	}
}

func selectFrom(_ string, _ string, _ string) []data.Row {
	rows := []data.Row{}
	return rows
}

func sort(rows []data.Row, orderBy string) []data.Row {
	sorted := sorter.SortableRows(rows).Sort(orderBy)
	return sorted
}

func resultSet(rows []data.Row) data.ResultSet {
	resultSet := data.ResultSet{
		Rows: rows,
		Statistics: data.Statistics{
			Count: 0, //rune(len(rows))
		},
	}
	return resultSet
}
