package storage

import (
	"slices"
	"sync"
)

type ColumnDefinition struct {
	Type       string
	Size       int
	Precision  int
	PrimaryKey bool
}

type CollectionDefinition struct {
	CollectionType string
	Columns        map[string]ColumnDefinition
	Sequence       int64
}

var (
	users              = []string{}
	collections        = make(map[string]CollectionDefinition)
	usersMapLock       = sync.Mutex{}
	collectionsMapLock = sync.Mutex{}
	collectionsRWLock  = make(map[string]*sync.RWMutex)
	sequences          = make(map[string]int64)
	sequencesMapLock   = sync.Mutex{}
)

func AcquireCollectionReadLock(collectionName string) {
	collectionsRWLock[collectionName].RLock()
}

func ReleaseCollectionReadLock(collectionName string) {
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

func HasSequenceColumn(collectionName string) bool {
	colDef := GetCollection(collectionName)
	for _, value := range colDef.Columns {
		if value.Type == "SEQUENCE" {
			return true
		}
	}
	return false
}

func AddUser(userName string) string {
	usersMapLock.Lock()
	defer usersMapLock.Unlock()
	if slices.Index(users, userName) >= 0 {
		return "ERROR - USER " + userName + " ALREADY EXISTS"
	}
	users = append(users, userName)
	return "OK - CREATED USER " + userName
}

func AddCollection(collectionName string, collectionType string, columns map[string]ColumnDefinition) string {
	collectionsMapLock.Lock()
	defer collectionsMapLock.Unlock()
	if _, hasObject := collections[collectionName]; hasObject {
		return "ERROR - " + collectionType + " " + collectionName + " ALREADY EXISTS"
	}
	collections[collectionName] = CollectionDefinition{CollectionType: collectionType, Columns: columns}
	collectionsRWLock[collectionName] = &sync.RWMutex{}
	if HasSequenceColumn(collectionName) {
		sequences[collectionName] = 0
	}
	return "OK - CREATED " + collectionType + " " + collectionName
}

func GetCollection(collectionName string) CollectionDefinition {
	return collections[collectionName]
}

func NewSequenceValue(collectionName string) int64 {
	if !HasSequenceColumn(collectionName) {
		return 0
	}
	sequencesMapLock.Lock()
	defer sequencesMapLock.Unlock()
	sequences[collectionName] = sequences[collectionName] + 1
	return sequences[collectionName]
}
