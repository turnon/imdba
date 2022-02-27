package table

import (
	"os"
	"strconv"
	"strings"
)

func generateInsertStmt(tableName string, columnNames []string, multiRows int) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")
	sb.WriteString(strings.Join(columnNames, ","))
	sb.WriteString(" ) VALUES ")

	var binding func()
	if os.Getenv("SQLITE") != "" {
		binding = func() {
			sb.WriteString("?")
		}
	} else if os.Getenv("PG") != "" {
		bindingCount := 0
		binding = func() {
			bindingCount += 1
			sb.WriteString("$")
			sb.WriteString(strconv.Itoa(bindingCount))
		}
	}

	for rows := multiRows; rows > 0; rows -= 1 {
		sb.WriteString("(")
		for cols := len(columnNames); cols > 0; cols -= 1 {
			binding()
			if cols > 1 {
				sb.WriteString(",")
			}
		}
		sb.WriteString(")")
		if rows > 1 {
			sb.WriteString(",")
		}
	}

	return sb.String()
}
