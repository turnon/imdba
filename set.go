package imsql

import "sort"

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

func (s set) sortedList() []string {
	strs := s.list()
	sort.Strings(strs)
	return strs
}
