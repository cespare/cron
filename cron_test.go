package cron

import (
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// One []int each for minutes, hours, ...
// Must be in sorted order. nil == '*'
type testSchedule [5][]int

func toTestSchedule(s Schedule) testSchedule {
	var ts testSchedule
	for i, size := range fieldSizes {
		var part []int
		allSet := true
		for j := 0; j < size; j++ {
			if s.isSet(fieldOffsets[i] + j) {
				part = append(part, j)
			} else {
				allSet = false
			}
		}
		if allSet {
			part = nil
		}
		ts[i] = part
	}
	return ts
}

func TestParseWithoutHash(t *testing.T) {
	for _, tt := range []struct {
		expr string
		want testSchedule
	}{
		{"* * * * *", testSchedule{nil, nil, nil, nil, nil}},
		{"0 0 1 1 0", testSchedule{{0}, {0}, {0}, {0}, {0}}},
		{"2,3 * * * *", testSchedule{{2, 3}, nil, nil, nil, nil}},
		{"2-5 * * * *", testSchedule{{2, 3, 4, 5}, nil, nil, nil, nil}},
		{"1,3-5 * * * *", testSchedule{{1, 3, 4, 5}, nil, nil, nil, nil}},
		{"1,3-5,10-45/10,58 * * * *", testSchedule{{1, 3, 4, 5, 10, 20, 30, 40, 58}, nil, nil, nil, nil}},
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
		s, err := Parse(tt.expr)
		if err != nil {
			t.Errorf("Parse(%q): %s", tt.expr, err)
			continue
		}
		if diff := cmp.Diff(toTestSchedule(s), tt.want); diff != "" {
			t.Errorf("Parse(%q): (-got, +want):\n%s", tt.expr, diff)
			continue
		}
		if strings.HasPrefix(tt.expr, "@") {
			continue
		}
		// ParseWithHash should return the same schedule as Parse when the
		// expression does not contain the H symbol.
		s, err = ParseWithHash(tt.expr, 0)
		if err != nil {
			t.Errorf("ParseWithHash(%q): %s", tt.expr, err)
			continue
		}
		if diff := cmp.Diff(toTestSchedule(s), tt.want); diff != "" {
			t.Errorf("ParseWithHash(%q): (-got, +want):\n%s", tt.expr, diff)
			continue
		}
	}
}

func TestParseFail(t *testing.T) {
	for _, tt := range []struct {
		expr string
		want string // substring
	}{
		{"* * * *", "wrong number of fields"},
		{"-1 * * * *", "invalid value"},
		{"60 * * * *", "invalid value"},
		{"* 24 * * *", "invalid value"},
		{"* * 0 * *", "invalid value"},
		{"* * 32 * *", "invalid value"},
		{"* * * 0 *", "invalid value"},
		{"* * * 13 *", "invalid value"},
		{"* * * J *", "invalid value"},
		{"* * * foo *", "invalid value"},
		{"* * * * 7", "invalid value"},
		{"1 - 3 * * * *", "wrong number of fields"},
		{"1-3-7 * * * *", "invalid value"},
		{"1/3/7 * * * *", "invalid increment"},
		{"@foobar", "unrecognized cron schedule"},
		{"H * * * *", `the "H" symbol`},
		{"* H/4 * * *", `the "H" symbol`},
		{"* 1,H/4 * * *", `the "H" symbol`},
		{"H(1-5) * * * *", "invalid value"},
	} {
		_, err := Parse(tt.expr)
		if err == nil {
			t.Errorf("Parse accepted %q, but it is invalid", tt.expr)
			continue
		}
		if !strings.Contains(err.Error(), tt.want) {
			t.Errorf("Parse(%q): got error %q; want substring %q", tt.expr, err, tt.want)
		}
	}
}

func TestParseWithHash(t *testing.T) {
	for _, tt := range []struct {
		expr     string
		randVals []int
		want     testSchedule
	}{
		{"@hourly", []int{10}, testSchedule{{10}, nil, nil, nil, nil}},
		{"H * * * *", []int{11}, testSchedule{{11}, nil, nil, nil, nil}},
		{"@daily", []int{12, 13}, testSchedule{{12}, {13}, nil, nil, nil}},
		{"H H * * *", []int{14, 15}, testSchedule{{14}, {15}, nil, nil, nil}},
		{"@weekly", []int{16, 17, 18}, testSchedule{{16}, {17}, nil, nil, {4}}},
		{"H H * * H", []int{19}, testSchedule{{19}, {19}, nil, nil, {5}}},
		{"@monthly", []int{27, 21}, testSchedule{{27}, {21}, {27}, nil, nil}},
		{"H H H * *", []int{28, 21}, testSchedule{{28}, {21}, {0}, nil, nil}},
		{"H 0 * * *", []int{22}, testSchedule{{22}, {0}, nil, nil, nil}},
		{"@weekly", []int{23, 24}, testSchedule{{23}, {0}, nil, nil, {2}}},
		{"H H H H H", []int{25, 26}, testSchedule{{25}, {2}, {25}, {2}, {4}}},
		{"H H H H H", []int{0}, testSchedule{{0}, {0}, {0}, {0}, {0}}},
		{"H H H H H", []int{59, 23, 27, 11, 6}, testSchedule{{59}, {23}, {27}, {11}, {6}}},
		{"H H H H H", []int{60, 24, 28, 12, 7}, testSchedule{{0}, {0}, {0}, {0}, {0}}},
		{"H H * * H", []int{3, 4, 5}, testSchedule{{3}, {4}, nil, nil, {5}}},
		{"H/1 * * * *", []int{3}, testSchedule{nil, nil, nil, nil, nil}},
		{"* H/6 * * *", []int{10}, testSchedule{nil, {4, 10, 16, 22}, nil, nil, nil}},
		{"H H/6 * * *", []int{11, 12}, testSchedule{{11}, {0, 6, 12, 18}, nil, nil, nil}},
		{"H/15 H/6 * * *", []int{64, 1}, testSchedule{{4, 19, 34, 49}, {1, 7, 13, 19}, nil, nil, nil}},
		{"H H/12 * March *", []int{14, 4}, testSchedule{{14}, {4, 16}, nil, {2}, nil}},
		{"H * * MARCH *", []int{14, 4}, testSchedule{{14}, nil, nil, {2}, nil}},
	} {
		s, err := parseWithHash(tt.expr, &fixedRNG{vals: tt.randVals})
		if err != nil {
			t.Errorf("parseWithHash(%q, %v): %s", tt.expr, tt.randVals, err)
			continue
		}
		if diff := cmp.Diff(toTestSchedule(s), tt.want); diff != "" {
			t.Errorf("parseWithHash(%q, %v): (-got, +want):\n%s", tt.expr, tt.randVals, diff)
			continue
		}
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

func TestNext(t *testing.T) {
	const layout = "2006-01-02 15:04"
	parseTime := func(s string) time.Time {
		t, err := time.Parse(layout, s)
		if err != nil {
			panic(err)
		}
		return t
	}
	for _, tt := range []struct {
		expr   string
		t1, t2 string
	}{
		{"* * * * *", "2014-01-01 00:00", "2014-01-01 00:01"},
		{"10 * * * *", "2014-01-01 00:00", "2014-01-01 00:10"},
		{"* 3 3 * *", "2014-01-01 00:00", "2014-01-03 03:00"},
		{"* * * SEP *", "2014-01-01 00:00", "2014-09-01 00:00"},
		// June is the first month with the 9th == monday
		{"* * 9 * Monday", "2014-01-01 00:00", "2014-06-09 00:00"},
	} {
		s, err := Parse(tt.expr)
		if err != nil {
			t.Errorf("Parse(%q): %s", tt.expr, err)
			continue
		}
		t1, t2 := parseTime(tt.t1), parseTime(tt.t2)
		got := s.Next(t1)
		if got != t2 {
			t.Errorf("got next(%q, %q) = %q; want %q", tt.expr, t1.Format(layout),
				got.Format(layout), t2.Format(layout))
		}
	}
}
