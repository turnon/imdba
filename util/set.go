package util

import (
	"regexp"
	"sort"
)

type Set map[string]struct{}

var emptyStruct = struct{}{}

func NewSet(strs ...string) Set {
	s := Set{}
	for _, str := range strs {
		s.Add(str)
	}
	return s
}

func (s Set) Add(str string) {
	if _, ok := s[str]; ok {
		return
	}
	s[str] = emptyStruct
}

func (s Set) Remove(str string) {
	delete(s, str)
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

func (s Set) IsSame(s2 Set) bool {
	if len(s) != len(s2) {
		return false
	}
	for key := range s {
		if _, ok := s2[key]; !ok {
			return false
		}
	}
	return true
}

func (s Set) AllMatch(rgexps ...*regexp.Regexp) bool {
	for key := range s {
		var match bool
		for _, re := range rgexps {
			if re.MatchString(key) {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	return true
}
