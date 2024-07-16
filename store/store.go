package store

type ColumnDefinition struct {
	ColumnType      string
	ColumnSize      int
	ColumnPrecision int
}

type CollectionDefinition struct {
	collectionType string
	columns        map[string]ColumnDefinition
}

var objects = make(map[string]string)

var collections = make(map[string]CollectionDefinition)

func AddObject(name string, objectType string) string {
	objType, hasObject := objects[name]
	if hasObject {
		if objType == objectType {
			return objectType + " " + name + " already exists"
		}
	}
	objects[name] = objectType
	return "Created " + objectType + name
}

func AddCollection(name string, collectionType string, _ map[string]string) string {
	objType, hasObject := objects[name]
	if hasObject {
		if objType == collectionType {
			return collectionType + " " + name + " already exists"
		}
	}
	objects[name] = collectionType
	collections[name] = CollectionDefinition{collectionType: collectionType}
	return "Created " + collectionType + name
}
