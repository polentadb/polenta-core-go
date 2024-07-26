package store

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
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
}

var (
	users              = []string{}
	collections        = make(map[string]CollectionDefinition)
	usersMapLock       = sync.Mutex{}
	collectionsMapLock = sync.Mutex{}
	collectionsRWLock  = make(map[string]*sync.RWMutex)
	sequences          = make(map[string]int64)
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
	fmt.Println("generating new sequence value", collectionName)
	if sequence, hasObject := sequences[collectionName]; hasObject {
		var inc int64 = 1
		fmt.Println("current value", collectionName, sequence)
		newValue := atomic.AddInt64(&sequence, inc)
		fmt.Println("new value", collectionName, newValue)
		return newValue
	}
	return 0
}
