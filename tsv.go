package imsql

import (
	"bufio"
	"os"
	"regexp"
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
