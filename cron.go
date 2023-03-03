// Package cron parses cron time schedules and computes scheduling.
package cron

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

// Parse parses a cron expression string. Five fields (minute, hour, day of
// month, month, day of week) are expected. Valid symbols are
//
//	/ - * ,
//
// Month and weekday names (or any unique prefix thereof, case-insensitively)
// may be used in those respective fields. Months start on day 1. Weeks start on
// day 0, Sunday. The maximum weekday value is 6, Saturday.
//
// Here are some examples of valid expression strings along with their meanings:
//
//   - "* * * * *": every minute
//   - "/5 * * * *": every 5 minutes
//   - "15 * * * *": every hour at 15 past
//   - "0 3 * * Wed": every Wednesday at 0300
//   - "0 0 1 */3 *": at the beginning of each quarter
//
// Instead of a five-field expression, a named schedule starting with "@" may be
// used. Four named schedules are recognized:
//
//   - "@monthly", meaning "0 0 1 * *",
//   - "@weekly", meaning "0 0 * * 0",
//   - "@daily", meaning "0 0 * * *", and
//   - "@hourly", meaning "0 * * * *".
//
// Read http://en.wikipedia.org/wiki/Cron for more information about the format.
func Parse(expr string) (Schedule, error) {
	if strings.HasPrefix(expr, "@") {
		e, ok := namedSchedules[expr]
		if !ok {
			return Schedule{}, fmt.Errorf("unrecognized cron schedule name: %q", expr)
		}
		expr = e
	}
	s, usesH, err := parseFields(expr, new(fixedRNG))
	if err != nil {
		return Schedule{}, err
	}
	if usesH {
		return Schedule{}, errors.New(`the "H" symbol cannot be used with Parse; use ParseWithHash instead`)
	}
	return s, nil
}

// ParseWithHash is like Parse but additionally supports the symbol H in place
// of the minute, hour, day of month, month, or day of week field. The H symbol
// requests a random value (within the valid range) for each instance of H in
// the cron expression fixed using the given seed.
//
// For example, the schedule
//
//	H H * * *
//
// is a schedule that fires once per day at a random hour and minute that is
// chosen when the schedule is parsed. Given the same seed, the same schedule is
// generated.
//
// The range for randomly generated day of month values is [1, 28].
//
// Additionally, ParseWithHash interprets the named schedules differently from
// Parse:
//
//   - "@monthly" means "H H H * *"
//   - "@weekly" means "H H * * H"
//   - "@daily" means "H H * * *"
//   - "@hourly" means "H * * * *"
//
// The idea of the H symbol is borrowed from Jenkins, though the details are a
// bit different.
func ParseWithHash(expr string, seed uint64) (Schedule, error) {
	return parseWithHash(expr, rand.New(rand.NewSource(int64(seed))))
}

type rng interface {
	Intn(n int) int
}

type fixedRNG struct {
	i    int
	vals []int
}

func (r *fixedRNG) Intn(n int) int {
	if len(r.vals) == 0 {
		r.vals = []int{0}
	}
	result := r.vals[r.i] % n
	r.i++
	if r.i == len(r.vals) {
		r.i = 0
	}
	return result
}

func parseWithHash(expr string, r rng) (Schedule, error) {
	if strings.HasPrefix(expr, "@") {
		e, ok := namedHashedSchedules[expr]
		if !ok {
			return Schedule{}, fmt.Errorf("unrecognized cron schedule name: %q", expr)
		}
		expr = e
	}
	s, _, err := parseFields(expr, r)
	return s, err
}

// Valid reports whether s is a valid schedule (that is, whether it could
// correspond to some well-formed cron expression).
func (s Schedule) Valid() bool {
outer:
	for i, size := range fieldSizes {
		for j := 0; j < size; j++ {
			if s.isSet(fieldOffsets[i] + j) {
				continue outer
			}
		}
		return false
	}
	return true
}

// Next gives the smallest time greater than t when the Schedule is satisfied.
// Next panics if s is not valid.
func (s Schedule) Next(t time.Time) time.Time {
	if !s.Valid() {
		panic("Next() called on invalid schedule")
	}
	// Start t off at the earliest possible subsequent minute.
	t = t.Truncate(time.Minute).Add(time.Minute)

	for {
		if !s.matchesMonth(t) {
			t = advanceMonth(t)
			continue
		}
		if !s.matchesDay(t) {
			t = advanceDay(t)
			continue
		}
		if !s.matchesHour(t) {
			t = advanceHour(t)
			continue
		}
		if !s.matchesMinute(t) {
			t = advanceMinute(t)
			continue
		}
		return t
	}
}

func advanceMonth(t time.Time) time.Time {
	year, month, _ := t.Date()
	return time.Date(year, month+1, 1, 0, 0, 0, 0, t.Location())
}

func advanceDay(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day+1, 0, 0, 0, 0, t.Location())
}

func advanceHour(t time.Time) time.Time {
	return t.Truncate(time.Hour).Add(time.Hour)
}

func advanceMinute(t time.Time) time.Time {
	return t.Truncate(time.Minute).Add(time.Minute)
}

func (s Schedule) matchesMonth(t time.Time) bool {
	return s.isSet(monthOffset + int(t.Month()) - 1)
}

func (s Schedule) matchesDay(t time.Time) bool {
	return s.isSet(domOffset+t.Day()-1) && s.isSet(dowOffset+int(t.Weekday()))
}

func (s Schedule) matchesHour(t time.Time) bool {
	return s.isSet(hourOffset + t.Hour())
}

func (s Schedule) matchesMinute(t time.Time) bool {
	return s.isSet(minuteOffset + t.Minute())
}

const (
	// These are in order, LSB first.
	minutes = 60
	hours   = 24
	doms    = 31
	months  = 12
	dows    = 7

	minuteOffset  = 0
	hourOffset    = minuteOffset + minutes
	domOffset     = hourOffset + hours
	monthOffset   = domOffset + doms
	dowOffset     = monthOffset + months
	end           = dowOffset + dows
	scheduleBytes = (end-1)/8 + 1
)

var fieldSizes = [...]int{
	0: minutes,
	1: hours,
	2: doms,
	3: months,
	4: dows,
}

var fieldOffsets = [...]int{
	0: minuteOffset,
	1: hourOffset,
	2: domOffset,
	3: monthOffset,
	4: dowOffset,
}

var fieldNames = [...]string{
	0: "minute",
	1: "hour",
	2: "day of month",
	3: "month",
	4: "day of week",
}

// A Schedule is a parsed cron schedule.
type Schedule struct {
	b [scheduleBytes]byte
}

var namedSchedules = map[string]string{
	"@monthly": "0 0 1 * *",
	"@weekly":  "0 0 * * 0",
	"@daily":   "0 0 * * *",
	"@hourly":  "0 * * * *",
}

var namedHashedSchedules = map[string]string{
	"@monthly": "H H H * *",
	"@weekly":  "H H * * H",
	"@daily":   "H H * * *",
	"@hourly":  "H * * * *",
}

var monthNames = []string{
	"january",
	"february",
	"march",
	"april",
	"may",
	"june",
	"july",
	"august",
	"september",
	"october",
	"november",
	"december",
}

var dowNames = []string{
	"sunday",
	"monday",
	"tuesday",
	"wednesday",
	"thursday",
	"friday",
	"saturday",
}

func parseFields(expr string, r rng) (s Schedule, usesH bool, err error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return Schedule{}, false, fmt.Errorf("wrong number of fields in schedule %q (expected 5)", expr)
	}
	for i, field := range fields {
		parts := strings.Split(field, ",")
		for _, part := range parts {
			partial, uh, err := parseSinglePart(part, i, r)
			if err != nil {
				return Schedule{}, false, err
			}
			if uh {
				if len(parts) > 1 {
					return Schedule{}, false, fmt.Errorf(`the "H" symbol is used with , in %q`, expr)
				}
				usesH = true
			}
			s = s.union(partial)
		}
	}
	return s, usesH, nil
}

func parseSinglePart(part string, fieldIndex int, r rng) (s Schedule, usesH bool, err error) {
	inc := 1
	incParts := strings.SplitN(part, "/", 2)
	if len(incParts) > 1 {
		var err error
		inc, err = strconv.Atoi(incParts[1])
		if err != nil {
			return Schedule{}, false, fmt.Errorf("invalid increment: %q", incParts[1])
		}
		if inc < 1 {
			return Schedule{}, false, fmt.Errorf("invalid increment %d (must be at least 1)", inc)
		}
	}
	var rangeStart, rangeEnd int // inclusive
	if incParts[0] == "*" {
		rangeStart = 0
		rangeEnd = fieldSizes[fieldIndex] - 1
	} else if strings.ToUpper(incParts[0]) == "H" {
		usesH = true
		n := fieldSizes[fieldIndex]
		if fieldIndex == 2 {
			// Only generate random days of the month in [1, 28].
			n = 28
		}
		if len(incParts) > 1 {
			// For interval schedules like H/n,
			// choose a random value less than n.
			if inc < n {
				n = inc
			}
			rangeStart = r.Intn(n)
			rangeEnd = fieldSizes[fieldIndex] - 1
		} else {
			rangeStart = r.Intn(n)
			rangeEnd = rangeStart
		}
	} else if rangeParts := strings.SplitN(incParts[0], "-", 2); len(rangeParts) == 2 {
		rangeStart, err = parseSingleValue(rangeParts[0], fieldIndex)
		if err != nil {
			return Schedule{}, false, err
		}
		rangeEnd, err = parseSingleValue(rangeParts[1], fieldIndex)
		if err != nil {
			return Schedule{}, false, err
		}
		if rangeStart == rangeEnd {
			return Schedule{}, false, fmt.Errorf("bad range %q -- start and end must be different", incParts[0])
		}
		// Compensate for the 1-indexed fields.
		switch fieldIndex {
		case 2, 3:
			rangeStart--
			rangeEnd--
		}
	} else {
		rangeStart, err = parseSingleValue(incParts[0], fieldIndex)
		if err != nil {
			return Schedule{}, false, err
		}
		// Compensate for the 1-indexed fields.
		switch fieldIndex {
		case 2, 3:
			rangeStart--
		}
		rangeEnd = rangeStart
	}

	var i int
	j := rangeStart
	for {
		if i%inc == 0 {
			s = s.set(fieldOffsets[fieldIndex] + j)
		}
		if j == rangeEnd {
			break
		}
		i++
		j++
		if j == fieldSizes[fieldIndex] {
			j = 0
		}
	}
	return s, usesH, nil
}

func parseSingleValue(val string, fieldIndex int) (int, error) {
	if n, err := strconv.Atoi(val); err == nil {
		switch fieldIndex {
		case 0, 1, 4:
			if n < 0 || n >= fieldSizes[fieldIndex] {
				goto badRange
			}
		case 2, 3:
			if n < 1 || n > fieldSizes[fieldIndex] {
				goto badRange
			}
		default:
			panic("unreachable")
		}
		return n, nil
	badRange:
		return 0, fmt.Errorf("invalid value %d for the %s field", n, fieldNames[fieldIndex])
	}
	switch fieldIndex {
	case 3:
		n := matchUniquePrefix(val, monthNames)
		if n >= 0 {
			return n + 1, nil
		}
	case 4:
		n := matchUniquePrefix(val, dowNames)
		if n >= 0 {
			return n, nil
		}
	}
	return 0, fmt.Errorf("invalid value %q for the %s field", val, fieldNames[fieldIndex])
}

func matchUniquePrefix(prefix string, dict []string) int {
	s := strings.ToLower(prefix)
	result := -1
	for i, s2 := range dict {
		if strings.HasPrefix(s2, s) {
			if result >= 0 {
				return -1
			}
			result = i
		}
	}
	return result
}

func (s Schedule) set(off int) Schedule {
	s.b[off/8] |= (1 << uint(off%8))
	return s
}

func (s Schedule) isSet(off int) bool {
	return s.b[off/8]&(1<<uint(off%8)) > 0
}

func (s Schedule) union(s1 Schedule) Schedule {
	for i := range s.b {
		s.b[i] |= s1.b[i]
	}
	return s
}
