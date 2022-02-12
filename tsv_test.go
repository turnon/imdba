package imsql

import (
	"sort"
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
	sort.Strings(strs)
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
		startYears.add(r.startYear)
		endYears.add(r.endYear)
		return nil
	})

	t.Log("start year", startYears.list())
	t.Log("end year", endYears.list())
}
