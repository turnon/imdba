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
	genres := set{}
	titleTypes := set{}
	var maxId uint

	IterateTitleBasic("title.basics.tsv", func(r *TitleBasicRow, err error) error {
		if err != nil {
			t.Error(err)
			return nil
		}

		if maxId < r.Id() {
			maxId = r.Id()
		}
		startYears.add(r.StartYear)
		endYears.add(r.EndYear)
		titleTypes.add(r.TitleType)
		for _, g := range r.GenresArray() {
			genres.add(g)
		}
		return nil
	})

	t.Log("start year", startYears.list())
	t.Log("end year", endYears.list())
	t.Log("title type", titleTypes.list())
	t.Log("genres", genres.list())
	t.Log("max id", maxId)
}
