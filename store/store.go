package store

import "sync"

type ColumnDefinition struct {
	ColumnType      string
	ColumnSize      int
	ColumnPrecision int
}

type CollectionDefinition struct {
	collectionType string
	columns        map[string]ColumnDefinition
}

var (
	objects         = make(map[string]string)
	collections     = make(map[string]CollectionDefinition)
	objectsLock     = sync.Mutex{}
	collectionsLock = sync.Mutex{}
)

func AddObject(name string, objectType string) string {
	objectsLock.Lock()
	defer objectsLock.Unlock()
	if objType, hasObject := objects[name]; hasObject && objType == objectType {
		return objectType + " " + name + " ALREADY EXISTS"
	}
	objects[name] = objectType
	return "CREATED " + objectType + " " + name
}

func AddCollection(name string, collectionType string, _ map[string]string) string {
	collectionsLock.Lock()
	defer collectionsLock.Unlock()
	if _, hasObject := collections[name]; hasObject {
		return collectionType + " " + name + " ALREADY EXISTS"
	}
	collections[name] = CollectionDefinition{collectionType: collectionType}
	return "CREATED " + collectionType + " " + name
}
