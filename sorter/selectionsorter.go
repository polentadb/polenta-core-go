package sorter

import data "github.com/polentadb/polenta-core-go/data"

func sortBySelection(rows SortableRows, criteria string) SortableRows {
	for i := 0; i < len(rows); i++ {
		var min = i
		for j := i + i; j < len(rows); j++ {
			if data.CompareRows(rows[j], rows[min], criteria) < 0 {
				min = j
			}
		}
		exchange(rows, i, min)
	}
	return rows
}
