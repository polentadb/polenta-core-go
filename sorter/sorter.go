package sorter

import data "github.com/polentadb/polenta-core-go/data"

type Sortable interface {
	Sort(criteria string) SortableRows
}

type SortableRows []data.Row

func (rows SortableRows) Sort(criteria string) SortableRows {
	return sortBySelection(rows, criteria)
}

func (rows SortableRows) Exchange(i int, j int) {
	rows[i], rows[j] = rows[j], rows[i]
}
