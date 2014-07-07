package cron

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Parse parses a cron expression string. Five fields (minute, hour, day of month, month, day of week) are
// expected. Valid symbols are
//
//   * , - /
//
// Month and weekday names (or any unique prefix thereof) may be used in those respective fields (case
// sensitivity is ignored).
//
// Read http://en.wikipedia.org/wiki/Cron for more information about the format.
func Parse(expr string) (*Schedule, error) {
	if fieldExpr, ok := namedSchedules[expr]; ok {
		return parseFields(fieldExpr)
	}
	schedule, err := parseFields(expr)
	if err != nil {
		// Just for a friendlier error message
		if strings.HasPrefix(expr, "@") {
			return nil, fmt.Errorf("unrecognized cron schedule name: %q\n", expr)
		}
		return nil, fmt.Errorf("invalid cron schedule: %s", err)
	}
	return schedule, nil
}

// Valid reports whether s is a valid schedule (that is, whether it could correspond to some well-formed cron
// expression).
func (s *Schedule) Valid() bool {
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

// Next gives the smallest time greater than t when the Schedule is satisfied. Next panics if s is not valid.
func (s *Schedule) Next(t time.Time) time.Time {
	if !s.Valid() {
		panic("Next() called on invalid schedule")
	}
	t = t.Truncate(time.Minute).Add(time.Minute) // Start t off at the earliest possible subsequent minute.

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

func advanceHour(t time.Time) time.Time { return t.Truncate(time.Hour).Add(time.Hour) }

func advanceMinute(t time.Time) time.Time { return t.Truncate(time.Minute).Add(time.Minute) }

func (s *Schedule) matchesMonth(t time.Time) bool { return s.isSet(monthOffset + int(t.Month()) - 1) }

func (s *Schedule) matchesDay(t time.Time) bool {
	return s.isSet(domOffset+t.Day()-1) && s.isSet(dowOffset+int(t.Weekday()))
}

func (s *Schedule) matchesHour(t time.Time) bool { return s.isSet(hourOffset + t.Hour()) }

func (s *Schedule) matchesMinute(t time.Time) bool { return s.isSet(minuteOffset + t.Minute()) }

const (
	// These are in order, LSB first
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

type Schedule [scheduleBytes]byte

var namedSchedules = map[string]string{
	"@monthly": "0 0 1 * *",
	"@weekly":  "0 0 * * 0",
	"@daily":   "0 0 * * *",
	"@hourly":  "0 * * * *",
}

var monthNames = []string{
	"january", "february", "march", "april", "may", "june", "july", "august", "september", "october",
	"november", "december",
}

var dowNames = []string{"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"}

// * / , -

func parseFields(expr string) (*Schedule, error) {
	var schedule Schedule
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("wrong number of fields in schedule %q (expected 5)", expr)
	}
	for i, field := range fields {
		for _, part := range strings.Split(field, ",") {
			partial, err := parseSinglePart(part, i)
			if err != nil {
				return nil, err
			}
			schedule.union(partial)
		}
	}
	return &schedule, nil
}

func parseSinglePart(part string, fieldIndex int) (*Schedule, error) {
	inc := 1
	incParts := strings.SplitN(part, "/", 2)
	if len(incParts) == 2 {
		var err error
		inc, err = strconv.Atoi(incParts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid increment: %q", incParts[1])
		}
		if inc < 1 {
			return nil, fmt.Errorf("invalid increment %d (must be at least 1)", inc)
		}
	}
	var rangeStart, rangeEnd int // inclusive
	var err error
	if incParts[0] == "*" {
		rangeStart = 0
		rangeEnd = fieldSizes[fieldIndex] - 1
	} else {
		if rangeParts := strings.SplitN(incParts[0], "-", 2); len(rangeParts) == 2 {
			rangeStart, err = parseSingleValue(rangeParts[0], fieldIndex)
			if err != nil {
				return nil, err
			}
			rangeEnd, err = parseSingleValue(rangeParts[1], fieldIndex)
			if err != nil {
				return nil, err
			}
			if rangeStart == rangeEnd {
				return nil, fmt.Errorf("bad range %q -- start and end must be different", incParts[0])
			}
		} else {
			rangeStart, err = parseSingleValue(incParts[0], fieldIndex)
			if err != nil {
				return nil, err
			}
			rangeEnd = rangeStart
		}
		// Compensate for the 1-indexed fields
		switch fieldIndex {
		case 2, 3:
			rangeStart--
			rangeEnd--
		}
	}

	var i int
	j := rangeStart
	first := false
	var s Schedule
	for {
		if i%inc == 0 {
			s.set(fieldOffsets[fieldIndex] + j)
		}
		if first {
			first = false
		} else if j == rangeEnd {
			break
		}
		i++
		j++
		if j == fieldSizes[fieldIndex] {
			j = 0
		}
	}
	return &s, nil
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

func (s *Schedule) set(off int) {
	(*s)[off/8] |= (1 << uint(off%8))
}

func (s *Schedule) isSet(off int) bool {
	return (*s)[off/8]&(1<<uint(off%8)) > 0
}

func (s *Schedule) union(other *Schedule) {
	for i := range *s {
		(*s)[i] |= (*other)[i]
	}
}

func (s *Schedule) String() string {
	var buf bytes.Buffer
	// TODO: The (undocumented?) b (binary) fmt formatter doesn't seem to work on bytes/uint8. Bug?
	for i, b := range *s {
		fmt.Fprintf(&buf, "%08b", uint32(b))
		if i < len(*s) {
			fmt.Fprint(&buf, " ")
		}
	}
	fmt.Fprintln(&buf)
	for i := 0; i < 5; i++ {
		anyUnset := false
		var indices []int
		for j := 0; j < fieldSizes[i]; j++ {
			if s.isSet(fieldOffsets[i] + j) {
				indices = append(indices, j)
			} else {
				anyUnset = true
			}
		}
		setList := fmt.Sprintf("%v", indices)
		if !anyUnset {
			setList = "*"
		}
		fmt.Fprintf(&buf, "%-15s %s\n", fieldNames[i], setList)
	}
	return buf.String()
}
