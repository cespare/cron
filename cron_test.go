package cron

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"
)

// One []int each for minutes, hours, ...
// Must be in sorted order. nil == '*'
type testSchedule [5][]int

func assertSchedule(t *testing.T, ts testSchedule, s *Schedule) {
	t.Helper()
	for i, size := range fieldSizes {
		set := ts[i]
		if set == nil {
			for j := 0; j < size; j++ {
				set = append(set, j)
			}
		}
		for j := 0; j < size; j++ {
			setInS := s.isSet(fieldOffsets[i] + j)
			setInTestCase := true
			index := sort.SearchInts(set, j)
			if index == len(set) || set[index] != j {
				setInTestCase = false
			}
			if setInS != setInTestCase {
				t.Fatalf("got %v; want %v\n", s, ts)
			}
		}
	}
}

type parseTestCase struct {
	expr   string
	parsed testSchedule
}

func TestParseWithoutHash(t *testing.T) {
	for _, testCase := range []parseTestCase{
		{"* * * * *", testSchedule{nil, nil, nil, nil, nil}},
		{"0 0 1 1 0", testSchedule{{0}, {0}, {0}, {0}, {0}}},
		{"2,3 * * * *", testSchedule{{2, 3}, nil, nil, nil, nil}},
		{"2-5 * * * *", testSchedule{{2, 3, 4, 5}, nil, nil, nil, nil}},
		{"1,3-5 * * * *", testSchedule{{1, 3, 4, 5}, nil, nil, nil, nil}},
		{
			"1,3-5,10-45/10,58 * * * *",
			testSchedule{{1, 3, 4, 5, 10, 20, 30, 40, 58}, nil, nil, nil, nil},
		},
		{"* 21-3 * * *", testSchedule{nil, {0, 1, 2, 3, 21, 22, 23}, nil, nil, nil}},
		{"* * * JAN *", testSchedule{nil, nil, nil, {0}, nil}},
		{"* * * Janua *", testSchedule{nil, nil, nil, {0}, nil}},
		{"* * * APR-JUL *", testSchedule{nil, nil, nil, {3, 4, 5, 6}, nil}},
		{"* * * * MON,WED", testSchedule{nil, nil, nil, nil, {1, 3}}},
		{"* */6 * * *", testSchedule{nil, {0, 6, 12, 18}, nil, nil, nil}},
		{"* 6-10/2 * * *", testSchedule{nil, {6, 8, 10}, nil, nil, nil}},
		{"@monthly", testSchedule{{0}, {0}, {0}, nil, nil}},
		{"@weekly", testSchedule{{0}, {0}, nil, nil, {0}}},
		{"@daily", testSchedule{{0}, {0}, nil, nil, nil}},
		{"@hourly", testSchedule{{0}, nil, nil, nil, nil}},
	} {
		t.Run(testCase.expr, func(t *testing.T) {
			s, err := Parse(testCase.expr)
			if err != nil {
				t.Fatal(err)
			}
			assertSchedule(t, testCase.parsed, s)
			// ParseWithHash should return the same schedule as Parse when the
			// expression does not contain the H symbol.
			if !strings.HasPrefix(testCase.expr, "@") {
				s, err = ParseWithHash(testCase.expr, rand.Uint64())
				if err != nil {
					t.Fatal(err)
				}
				assertSchedule(t, testCase.parsed, s)
			}
		})
	}
}

func TestParseFail(t *testing.T) {
	for _, tt := range []struct {
		expr string
		want error // nil for any error
	}{
		{"* * * *", nil},
		{"-1 * * * *", nil},
		{"60 * * * *", nil},
		{"* 24 * * *", nil},
		{"* * 0 * *", nil},
		{"* * 32 * *", nil},
		{"* * * 0 *", nil},
		{"* * * 13 *", nil},
		{"* * * J *", nil},
		{"* * * foo *", nil},
		{"* * * * 7", nil},
		{"1 - 3 * * * *", nil},
		{"1-3-7 * * * *", nil},
		{"1/3/7 * * * *", nil},
		{"@foobar", nil},
		{"H * * * *", ErrParseHashedSchedule},
		{"* H/4 * * *", ErrParseHashedSchedule},
		{"* 1,H/4 * * *", ErrParseHashedSchedule},
		{"H(1-5) * * * *", nil},
	} {
		t.Run(tt.expr, func(t *testing.T) {
			_, err := Parse(tt.expr)
			if err == nil {
				t.Fatalf("Parse accepted %q, but it is invalid", tt.expr)
			}
			if tt.want != nil && err != tt.want {
				t.Errorf("Parse returned an unexpected error: %s", err)
			}
		})
	}
}

type parseHashedTestCase struct {
	expr   string
	seed   uint64
	parsed testSchedule
}

func TestParseWithHash(t *testing.T) {
	for _, testCase := range []parseHashedTestCase{
		{"@hourly", 1, testSchedule{{41}, nil, nil, nil, nil}},
		{"H * * * *", 1, testSchedule{{41}, nil, nil, nil, nil}},
		{"@daily", 1, testSchedule{{41}, {15}, nil, nil, nil}},
		{"H H * * *", 1, testSchedule{{41}, {15}, nil, nil, nil}},
		{"@weekly", 1, testSchedule{{41}, {15}, nil, nil, {6}}},
		{"H H * * H", 1, testSchedule{{41}, {15}, nil, nil, {6}}},
		{"@monthly", 1, testSchedule{{41}, {15}, {0}, nil, nil}},
		{"H H H * *", 1, testSchedule{{41}, {15}, {0}, nil, nil}},

		{"H 0 * * *", 1, testSchedule{{41}, {0}, nil, nil, nil}},
		{"@weekly", 10, testSchedule{{14}, {16}, nil, nil, {5}}},
		{"H H H H H", 10, testSchedule{{14}, {16}, {11}, {11}, {5}}},
		{"H H * * H", 100, testSchedule{{43}, {8}, nil, nil, {1}}},
		{"@monthly", 100, testSchedule{{43}, {8}, {11}, nil, nil}},
		{"H/1 * * * *", 1, testSchedule{nil, nil, nil, nil, nil}},
		{"* H/6 * * *", 10, testSchedule{nil, {4, 10, 16, 22}, nil, nil, nil}},
		{"H H/6 * * *", 10, testSchedule{{14}, {4, 10, 16, 22}, nil, nil, nil}},
		{"H/15 H/6 * * *", 10, testSchedule{{4, 19, 34, 49}, {1, 7, 13, 19}, nil, nil, nil}},
		{"H H/12 * MARCH *", 10, testSchedule{{14}, {4, 16}, nil, {2}, nil}},
		{"H * * MARCH *", 10, testSchedule{{14}, nil, nil, {2}, nil}},
	} {
		t.Run(fmt.Sprintf("%s %d", testCase.expr, testCase.seed), func(t *testing.T) {
			s, err := ParseWithHash(testCase.expr, testCase.seed)
			if err != nil {
				t.Fatal(err)
			}
			assertSchedule(t, testCase.parsed, s)
		})
	}
}

func TestValid(t *testing.T) {
	s, err := Parse("* * * * *")
	if err != nil {
		t.Fatal(err)
	}
	if !s.Valid() {
		t.Fatalf("expected %v to be valid", s)
	}
	if new(Schedule).Valid() {
		t.Fatal("a blank schedule should not be valid")
	}
}

type nextTestCase struct {
	expr   string
	t1, t2 string
}

const testTimeLayout = "2006-01-02 15:04"

func TestNext(t *testing.T) {
	for _, testCase := range []nextTestCase{
		{"* * * * *", "2014-01-01 00:00", "2014-01-01 00:01"},
		{"10 * * * *", "2014-01-01 00:00", "2014-01-01 00:10"},
		{"* 3 3 * *", "2014-01-01 00:00", "2014-01-03 03:00"},
		{"* * * SEP *", "2014-01-01 00:00", "2014-09-01 00:00"},
		// June is the first month with the 9th == monday
		{"* * 9 * Monday", "2014-01-01 00:00", "2014-06-09 00:00"},
	} {
		s, err := Parse(testCase.expr)
		if err != nil {
			t.Fatal(err)
		}
		t1, err := time.Parse(testTimeLayout, testCase.t1)
		if err != nil {
			t.Fatal(err)
		}
		t2, err := time.Parse(testTimeLayout, testCase.t2)
		if err != nil {
			t.Fatal(err)
		}
		got := s.Next(t1)
		if got != t2 {
			t.Errorf("got next(%q, %q) = %q; want %q", testCase.expr, t1.Format(testTimeLayout),
				got.Format(testTimeLayout), t2.Format(testTimeLayout))
		}
	}
}
