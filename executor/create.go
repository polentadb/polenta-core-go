package executor

import (
	store "github.com/polentadb/polenta-core-go/store"
	"strconv"
	"strings"
)

type CreateExecutor struct {
	statement string
}

func (s CreateExecutor) Execute() Response {
	return Response{Message: execute(s.statement)}
}

func execute(statement string) string {
	objectType := findObjectType(statement)
	objectName := findObjectName(statement)
	if objectType == "BAG" || objectType == "TABLE" {
		columns := findCollectionColumns(statement)
		return store.AddCollection(objectName, objectType, columns)
	} else if objectType == "USER" {
		return store.AddUser(objectName)
	} else {
		return "ERROR - OBJECT TYPE " + objectType + " NOT SUPPORTED"
	}
}

func findObjectName(statement string) string {
	parts := strings.Split(strings.ToUpper(statement), " ")
	return strings.Trim(parts[2], " ")
}

func findObjectType(statement string) string {
	parts := strings.Split(strings.ToUpper(statement), " ")
	return strings.Trim(parts[1], " ")
}

func findCollectionColumns(sql string) map[string]store.ColumnDefinition {
	columns := make(map[string]string)
	upSql := strings.TrimSpace(strings.ToUpper(sql))
	firstParenthesis := strings.Index(upSql, "(")
	parts := parts(upSql[firstParenthesis+1 : len(upSql)-1])
	for _, part := range parts {
		spaceIndex := strings.Index(part, " ")
		fieldName := strings.ReplaceAll(strings.TrimSpace(part[:spaceIndex]), " ", "")
		fieldType := strings.ReplaceAll(strings.TrimSpace(part[spaceIndex:]), " ", "")
		columns[fieldName] = fieldType
	}
	return columnsAsColumnsDef(columns)
}

func columnsAsColumnsDef(columns map[string]string) map[string]store.ColumnDefinition {
	columnsDef := make(map[string]store.ColumnDefinition)
	for key, value := range columns {
		var columnType string
		size := 0
		precision := 0
		if strings.Contains(value, "(") {
			columnType = strings.Split(value, "(")[0]
			columnTypeDetails := strings.Split(value, "(")[1]
			columnTypeDetails = columnTypeDetails[:len(columnTypeDetails)-1]
			if strings.Contains(columnTypeDetails, ",") {
				s, _ := strconv.Atoi(strings.Split(columnTypeDetails, ",")[0])
				p, _ := strconv.Atoi(strings.Split(columnTypeDetails, ",")[1])
				size = s
				precision = p
			} else {
				s, _ := strconv.Atoi(columnTypeDetails)
				size = s
			}
		} else {
			columnType = value
		}
		columnsDef[key] = store.ColumnDefinition{Type: columnType, Size: size, Precision: precision}
	}
	return columnsDef
}

func parts(subSql string) []string {
	parts := []string{}
	cleanedSubSql := strings.ReplaceAll(strings.TrimSpace(strings.ToUpper(subSql)), ",", ", ")
	previous := ""
	for _, part := range strings.Split(cleanedSubSql, ",") {
		if !strings.Contains(part, "FLOAT") {
			if strings.Contains(previous, "FLOAT") {
				parts = append(parts, strings.TrimSpace(previous+","+part))
			} else {
				parts = append(parts, strings.TrimSpace(part))
			}
		}
		previous = part
	}
	return parts
}
