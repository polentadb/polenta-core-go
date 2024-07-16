package store

type objectDefinition struct {
	objectType string
	fields     map[string]string
}

type Storable interface {
}

var store map[string]objectDefinition = make(map[string]objectDefinition)

func Add(name string, objectType string, fields map[string]string) {
	var objectDef objectDefinition
	objectDef.objectType = objectType
	objectDef.fields = fields

	store[name] = objectDef
}
