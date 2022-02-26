package tsv

import (
	"bufio"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var tIdRegexp = regexp.MustCompile(`tt[0]*`)
var nIdRegexp = regexp.MustCompile(`nm[0]*`)

const tab = "\t"

func LoopTsv(tsvPath string, yield func(uint, []string) error) error {
	f, err := os.Open(tsvPath)
	if err != nil {
		return err
	}
	defer f.Close()

	fileScanner := bufio.NewScanner(f)
	fileScanner.Split(bufio.ScanLines)

	var lineno uint
	for fileScanner.Scan() {
		lineno = lineno + 1
		if lineno == 1 {
			continue
		}
		line := fileScanner.Text()
		rec := strings.Split(line, tab)

		if yield(lineno, rec) != nil {
			break
		}
	}

	return nil
}

func CountLine(tsv string) (uint, error) {
	var lineCount uint
	f, err := os.Open(tsv)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		lineCount++
	}
	return lineCount - 1, nil
}

func tt2Int(tt string) uint {
	idStr := tIdRegexp.ReplaceAllString(tt, "")
	i, _ := strconv.ParseUint(idStr, 0, 32)
	return uint(i)
}
