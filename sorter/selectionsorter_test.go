package sorter

import (
	data "github.com/polentadb/polenta-core-go/data"
	"testing"
)

func TestSelectionSort(t *testing.T) {
	r1 := data.Row{"name": data.NewStringValue("John"), "age": data.NewIntegerValue(25)}
	r2 := data.Row{"name": data.NewStringValue("Paul"), "age": data.NewIntegerValue(20)}
	r3 := data.Row{"name": data.NewStringValue("Ringo"), "age": data.NewIntegerValue(27)}
	r := []data.Row{r1, r2, r3}
	sr := SortableRows(r)
	sorted := sortBySelection(sr, "age")
	if sorted[0].GetString("name") != "Paul" {
		t.Error("Expected Paul, got ", sorted[0].GetString("name"))
	}
	if sorted[1].GetString("name") != "John" {
		t.Error("Expected John, got ", sorted[1].GetString("name"))
	}
	if sorted[2].GetString("name") != "Ringo" {
		t.Error("Expected Ringo, got ", sorted[2].GetString("name"))
	}
}
