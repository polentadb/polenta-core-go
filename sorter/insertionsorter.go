package sorter

import data "github.com/polentadb/polenta-core-go/data"

func sortByInsertion(rows SortableRows, criteria string) SortableRows {
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0 && data.CompareRows(rows[j], rows[j-1], criteria) < 0; j-- {
			exchange(rows, j, j-1)
		}
	}
	return rows
}
