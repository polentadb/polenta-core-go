package data

type Statistics struct {
	Count int
}

type ResultSet struct {
	Rows       []Row
	Statistics Statistics
}
