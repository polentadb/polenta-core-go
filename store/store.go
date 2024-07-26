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
	Sequence       int64
}

var (
	users              = []string{}
	collections        = make(map[string]CollectionDefinition)
	usersMapLock       = sync.Mutex{}
	collectionsMapLock = sync.Mutex{}
	collectionsRWLock  = make(map[string]*sync.RWMutex)
	//sequences          = make(map[string]int64)
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
	return "OK - CREATED " + collectionType + " " + collectionName
}

func GetCollection(collectionName string) CollectionDefinition {
	return collections[collectionName]
}

func NewSequenceValue(collectionName string) int64 {
	if !HasSequenceColumn(collectionName) {
		return 0
	}
	fmt.Println("generating new sequence value", collectionName)
	var inc int64 = 1
	ptr1 := collections[collectionName]
	ptr2 := &ptr1.Sequence
	fmt.Println("current value", collectionName, ptr1.Sequence)
	newValue := atomic.AddInt64(ptr2, inc)
	fmt.Println("new value", collectionName, newValue)
	fmt.Println("seq value", collectionName, ptr1.Sequence)
	return newValue
}
