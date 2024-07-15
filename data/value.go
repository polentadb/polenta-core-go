package data

type Value interface{}

type IntegerValue struct {
	value int
}

type StringValue struct {
	value string
}

func NewIntegerValue(v int) IntegerValue {
	return IntegerValue{value: v}
}

func NewStringValue(v string) StringValue {
	return StringValue{value: v}
}

func (v IntegerValue) Get() int {
	return v.value
}

func (v StringValue) Get() string {
	return v.value
}

func CompareValues(v1 Value, v2 Value) int {
	switch v1.(type) {
	case IntegerValue:
		return v1.(IntegerValue).Compare(v2.(IntegerValue))
	case StringValue:
		return v2.(StringValue).Compare(v2.(StringValue))
	default:
		return 0
	}
}

func (v1 IntegerValue) Compare(v2 IntegerValue) int {
	if v1.value < v2.value {
		return -1
	} else if v1.value > v2.value {
		return 1
	} else {
		return 0
	}
}

func (v1 StringValue) Compare(v2 StringValue) int {
	if v1.value < v2.value {
		return -1
	} else if v1.value > v2.value {
		return 1
	} else {
		return 0
	}
}
