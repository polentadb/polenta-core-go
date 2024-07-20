package store

import (
	"fmt"
	"sync"
)

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
	objects            = make(map[string]string)
	collections        = make(map[string]CollectionDefinition)
	objectsMapLock     = sync.Mutex{}
	collectionsMapLock = sync.Mutex{}
	collectionsRWLock  = make(map[string]*sync.RWMutex)
)

func AcquireCollectionReadLock(collectionName string) {
	fmt.Println("acquire lock for " + collectionName)
	collectionsRWLock[collectionName].RLock()
}

func ReleaseCollectionReadLock(collectionName string) {
	fmt.Println("release lock for " + collectionName)
	collectionsRWLock[collectionName].RUnlock()
}

func AcquireCollectionWriteLock(collectionName string) {
	collectionsRWLock[collectionName].Lock()
}

func ReleaseCollectionWriteLock(collectionName string) {
	collectionsRWLock[collectionName].Unlock()
}

func HasCollection(collectionName string) bool {
	_, ok := collections[collectionName]
	return ok
}

func AddObject(name string, objectType string) string {
	objectsMapLock.Lock()
	defer objectsMapLock.Unlock()
	if objType, hasObject := objects[name]; hasObject && objType == objectType {
		return "ERROR - " + objectType + " " + name + " ALREADY EXISTS"
	}
	objects[name] = objectType
	return "OK - CREATED " + objectType + " " + name
}

func AddCollection(name string, collectionType string, _ map[string]string) string {
	collectionsMapLock.Lock()
	defer collectionsMapLock.Unlock()
	if _, hasObject := collections[name]; hasObject {
		return "ERROR - " + collectionType + " " + name + " ALREADY EXISTS"
	}
	collections[name] = CollectionDefinition{collectionType: collectionType}
	collectionsRWLock[name] = &sync.RWMutex{}
	return "OK - CREATED " + collectionType + " " + name
}
