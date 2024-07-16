package executor

import (
	data "github.com/polentadb/polenta-core-go/data"
	sorter "github.com/polentadb/polenta-core-go/sorter"
	"strconv"
)

type SelectExecutor struct {
	statement string
}

func (s SelectExecutor) Execute() Response {
	source := "TBD"
	fields := "TBD"
	where := "TBD"
	orderBy := "TBD"
	//rows := data.Rows{}
	selected := selectFrom(source, fields, where)
	sorted := sort(selected, orderBy)
	resultSet := resultSet(sorted)

	return Response{Message: "Executed select statement. Selected " + strconv.Itoa(resultSet.Statistics.Count) + " rows."}
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
