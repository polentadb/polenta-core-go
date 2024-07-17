package sorter

import data "github.com/polentadb/polenta-core-go/data"

func sortBySelection(rows SortableRows, criteria string) SortableRows {
	for i := 0; i < len(rows); i++ {
		var minI = i
		for j := i + i; j < len(rows); j++ {
			if data.CompareRows(rows[j], rows[minI], criteria) < 0 {
				minI = j
			}
		}
		rows.Exchange(i, minI)
	}
	return rows
}
