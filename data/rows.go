package data

type Statistics struct {
	count int
}

type ResultSet struct {
	Rows       []Row
	Statistics Statistics
}
