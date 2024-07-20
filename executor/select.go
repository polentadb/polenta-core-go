package executor

import (
	"fmt"
	data "github.com/polentadb/polenta-core-go/data"
	sorter "github.com/polentadb/polenta-core-go/sorter"
	store "github.com/polentadb/polenta-core-go/store"
	"strconv"
	"strings"
)

type SelectExecutor struct {
	statement string
}

func (s SelectExecutor) Execute() Response {
	collection := findCollectionName(s.statement)

	if !store.HasCollection(collection) {
		return Response{Error: fmt.Sprintf("No such bag or table: %s", collection)}
	}

	//store.AcquireCollectionReadLock(collection)
	//defer store.ReleaseCollectionReadLock(collection)

	fields := "TBD"
	where := "TBD"
	orderBy := "TBD"
	//rows := data.Rows{}
	selected := selectFrom(collection, fields, where)
	sorted := sort(selected, orderBy)
	resultSet := resultSet(sorted)

	return Response{Message: "EXECUTED SELECT STATEMENT. SELECTED " + strconv.Itoa(resultSet.Statistics.Count) + " ROWS."}
}

func findCollectionName(sql string) string {
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
