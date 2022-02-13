package tsv

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/turnon/imdba/util"
)

var yearRe = regexp.MustCompile(`^[0-9]{4}$`)
var nullRe = regexp.MustCompile(`^\\N$`)

func TestIterateTitleBasic(t *testing.T) {
	const tsv = "testdata/title.basics.tsv"
	tsLinesCount, err := CountLine(tsv)
	if err != nil {
		t.Errorf("fail to count lines in %s", tsv)
	}

	startYears := util.Set{}
	endYears := util.Set{}
	genres := util.Set{}
	titleTypes := util.Set{}
	ids := util.Set{}
	var count uint

	IterateTitleBasic(tsv, func(r *TitleBasicRow) error {
		count += 1
		ids.Add(strconv.FormatUint(uint64(r.Id()), 10))
		startYears.Add(r.StartYear)
		endYears.Add(r.EndYear)
		titleTypes.Add(r.TitleType)
		genres.AddAll(r.GenresArray())

		return nil
	})

	if !startYears.AllMatch(yearRe, nullRe) {
		t.Error("wrong start years", startYears.SortedList())
	}

	if !endYears.AllMatch(yearRe, nullRe) {
		t.Error("wrong start years", endYears.SortedList())
	}

	expectedGenres := util.NewSet(strings.Split("Action Adult Adventure Animation Biography Comedy Crime Documentary Drama Family Fantasy Film-Noir"+" "+
		"Game-Show History Horror Music Musical Mystery News Reality-TV Romance Sci-Fi Short Sport Talk-Show Thriller War Western \\N", " ")...)
	if !genres.IsSame(expectedGenres) {
		t.Errorf("wrong genres, expected %d, actual: %d", len(expectedGenres), len(genres))
	}

	expectedTitleTypes := util.NewSet(strings.Split("movie short tvEpisode tvMiniSeries tvMovie tvPilot tvSeries tvShort tvSpecial video videoGame", " ")...)
	if !titleTypes.IsSame(expectedTitleTypes) {
		t.Errorf("wrong genres, expected %d, actual: %d", len(expectedTitleTypes), len(titleTypes))
	}

	if idCount := uint(len(ids)); idCount != tsLinesCount {
		t.Errorf("wrong id count, expected %d, actual: %d", tsLinesCount, idCount)
	}

	if count != tsLinesCount {
		t.Errorf("wrong line count, expected %d, actual: %d", tsLinesCount, count)
	}
}

func TestIterateNameBasic(t *testing.T) {
	const tsv = "testdata/name.basics.tsv"
	tsLinesCount, err := CountLine(tsv)
	if err != nil {
		t.Errorf("fail to count lines in %s", tsv)
	}

	birthYears := util.Set{}
	deathYears := util.Set{}
	professions := util.Set{}
	ids := util.Set{}
	var count uint

	IterateNameBasic(tsv, func(r *NameBasicRow) error {
		count += 1
		ids.Add(strconv.FormatUint(uint64(r.Id()), 10))
		birthYears.Add(r.BirthYear)
		deathYears.Add(r.DeathYear)
		professions.AddAll(r.PrimaryProfessionArray())

		return nil
	})

	if !birthYears.AllMatch(yearRe, nullRe) {
		t.Error("wrong start years", birthYears.SortedList())
	}

	if !deathYears.AllMatch(yearRe, nullRe) {
		t.Error("wrong start years", deathYears.SortedList())
	}

	professions.Remove("")
	expectedProfessions := util.NewSet(strings.Split("actor actress animation_department art_department art_director assistant assistant_director camera_department"+" "+
		"casting_department casting_director choreographer cinematographer composer costume_department costume_designer director editor editorial_department"+" "+
		"electrical_department executive legal location_management make_up_department manager miscellaneous music_department producer production_department"+" "+
		"production_designer production_manager publicist script_department set_decorator sound_department soundtrack special_effects stunts talent_agent"+" "+
		"transportation_department visual_effects writer", " ")...)
	if !professions.IsSame(expectedProfessions) {
		t.Errorf("wrong professions, expected %d, actual: %d", len(expectedProfessions), len(professions))
	}

	if idCount := uint(len(ids)); idCount != tsLinesCount {
		t.Errorf("wrong id count, expected %d, actual: %d", tsLinesCount, idCount)
	}

	if count != tsLinesCount {
		t.Errorf("wrong line count, expected %d, actual: %d", tsLinesCount, count)
	}
}

func TestIterateTitlePrincipal(t *testing.T) {
	const tsv = "testdata/title.principals.tsv"
	tsLinesCount, err := CountLine(tsv)
	if err != nil {
		t.Errorf("fail to count lines in %s", tsv)
	}

	categories := util.Set{}
	expectedCategories := util.NewSet(strings.Split("actor actress archive_footage archive_sound cinematographer composer director editor producer production_designer self writer", " ")...)
	var count uint

	IterateTitlePrincipal(tsv, func(r *TitlePrincipalRow) error {
		count += 1
		categories.Add(r.Category)
		return nil
	})

	if !categories.IsSame(expectedCategories) {
		t.Errorf("wrong categories, expected %d, actual: %d", len(expectedCategories), len(categories))
	}

	if count != tsLinesCount {
		t.Errorf("wrong line count, expected %d, actual: %d", tsLinesCount, count)
	}
}
