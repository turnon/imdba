package imsql

import (
	"testing"
)

type set map[string]struct{}

func (s set) add(str string) {
	if _, ok := s[str]; ok {
		return
	}
	s[str] = struct{}{}
}

func (s set) list() []string {
	strs := []string{}
	for key := range s {
		strs = append(strs, key)
	}
	return strs
}

func TestIterateTitleBasic(t *testing.T) {
	startYears := set{}
	endYears := set{}
	IterateTitleBasic("title.basics.tsv", func(r *TitleBasicRow, err error) error {
		if err != nil {
			t.Error(err)
			return nil
		}
		// if r.lineNo > 100000 {
		// 	return errors.New("")
		// }
		startYears.add(r.startYear)
		endYears.add(r.endYear)
		return nil
	})
	t.Log(startYears.list(), endYears.list())
}
