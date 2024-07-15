package data

type Row map[string]Value

func (r Row) Get(field string) Value {
	v := r[field]
	return v
}

func (r Row) GetInt(field string) int {
	v1 := r[field]
	return v1.(IntegerValue).Get()
}

func (r Row) GetString(field string) string {
	v1 := r[field]
	return v1.(StringValue).Get()
}

func CompareRows(row1 Row, row2 Row, field string) int {
	value1 := row1[field]
	value2 := row2[field]
	return CompareValues(value1, value2)
}
