package util

import "sort"

type Set map[string]struct{}

func (s Set) Add(str string) {
	if _, ok := s[str]; ok {
		return
	}
	s[str] = struct{}{}
}

func (s Set) List() []string {
	strs := []string{}
	for key := range s {
		strs = append(strs, key)
	}
	return strs
}

func (s Set) SortedList() []string {
	strs := s.List()
	sort.Strings(strs)
	return strs
}
