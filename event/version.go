package event

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
)

const (
	VersionPartsCount = 2

	PartZeroStr = "00000000000000000000"
	FracZeroStr = "0000000000"
)

var (
	ErrVersionLimitExceeded = errors.New("version limit exceeded")
	ErrVersionMalformed     = errors.New("version malformed")
	ErrInvalidIncrSequence  = errors.New("invalid increment sequence")
)

var (
	VersionMax = Version{
		ps: [VersionPartsCount]uint64{
			math.MaxUint64,
			math.MaxUint64,
		},
		d: math.MaxUint32,
	}

	VersionMin = Version{
		ps: [VersionPartsCount]uint64{
			1, 0,
		},
		d: 0,
	}

	VersionZero = Version{
		ps: [VersionPartsCount]uint64{
			0, 0,
		},
		d: 0,
	}
)

type VersionSequenceDiff int

const (
	VersionSeqDiffFrac10p0 VersionSequenceDiff = iota
	VersionSeqDiffFrac10p1
)
const (
	VersionSeqDiff10p0 VersionSequenceDiff = 10 + iota
)

type Version struct {
	// integer parts
	ps [2]uint64
	// fractional part
	d uint32
}

func Ver(str ...string) (Version, error) {
	if len(str) > 0 {
		return ParseVersion(str[0])
	}
	return VersionMin, nil
}

func NewVersion() Version {
	return VersionMin
}

func ParseVersion(vstr string) (Version, error) {
	l := len(vstr)
	if l == 0 {
		return VersionZero, nil
	}

	v := Version{}
	if l%20 != 11 || vstr[l-11:l-10] != "." {
		return v, fmt.Errorf("%w: %s", ErrVersionMalformed, vstr)
	}

	if dstr := vstr[l-10 : l]; dstr != FracZeroStr {
		d, err := strconv.ParseUint(dstr, 10, 32)
		if err != nil {
			return v, fmt.Errorf("%w: %v", ErrVersionMalformed, err)
		}
		v.d = uint32(d)
	}

	q := (l - 11) / 20
	if q > VersionPartsCount {
		return v, fmt.Errorf("%w: %s", ErrVersionMalformed, vstr)
	}
	for i := 0; i < q; i++ {
		pstr := vstr[20*i : 20*(i+1)]
		if pstr == PartZeroStr {
			continue
		}
		p, err := strconv.ParseUint(pstr, 10, 64)
		if err != nil {
			return v, fmt.Errorf("%w: %v", ErrVersionMalformed, err)
		}
		v.ps[i] = uint64(p)
	}

	return v, nil
}

// Incr increments the version integer part while removing the fractional part from the returned version
// It may panic if one of parts limit is exceeded
func (v Version) Incr() Version {
	return v.doIncr(VersionSeqDiff10p0)
}

// Decr decrements the version integer part while removing the fractional part from the returned version
// It may panic if one of parts limit is exceeded
func (v Version) Decr() Version {
	for i := VersionPartsCount - 1; i >= 0; i-- {
		if v.ps[i] > 0 {
			v.ps[i]--
			v.d = 0
			break
		} else {
			if i == 0 {
				panic(ErrVersionLimitExceeded)
			}
		}
	}
	return v
}

// doIncr is used internally and mainly by Incr func and Envelop Options
// It's more capable and support incrementing based on different Sequence different base
// It allows forcing some incr convension like the one used in sourcing streams aka versioned-based streams
func (v Version) doIncr(diff VersionSequenceDiff) Version {
	switch diff {
	case VersionSeqDiffFrac10p0:
		if v.d < math.MaxUint32-uint32(1) {
			v.d += 1
			return v
		} else {
			panic(ErrVersionLimitExceeded)
		}
	case VersionSeqDiffFrac10p1:
		if v.d < math.MaxUint32-uint32(10) {
			v.d += 10
			return v
		} else {
			panic(ErrVersionLimitExceeded)
		}
	case VersionSeqDiff10p0:
		for i := 0; i < VersionPartsCount; i++ {
			if v.ps[i] < math.MaxUint64 {
				v.ps[i]++
				v.d = 0
				break
			} else {
				if i == VersionPartsCount-1 {
					panic(ErrVersionLimitExceeded)
				}
			}
		}
		return v
	default:
		panic(fmt.Errorf("%w: %d", ErrInvalidIncrSequence, diff))
	}
}

// Add returns a new version that
func (v Version) Add(p uint64, d uint32) Version {
	if p > 0 {
		for i := 0; i < VersionPartsCount; i++ {
			if v.ps[i] <= math.MaxUint64-p {
				v.ps[i] += p
				break
			} else {
				if i == VersionPartsCount-1 {
					panic(ErrVersionLimitExceeded)
				} else {
					p = p + v.ps[i] - math.MaxUint64
					v.ps[i] = math.MaxUint64
				}
			}
		}
	}
	if d > 0 {
		if v.d <= math.MaxUint32-d {
			v.d += d
		} else {
			panic(ErrVersionLimitExceeded)
		}
	}
	return v
}

// Trunc returns a new version without the fractional part
// it has a similar effect to floor function in most programming languages' math libraries
func (v Version) Trunc() Version {
	v.d = 0
	return v
}

func (v Version) String() string {
	var buf bytes.Buffer
	for i, p := range v.ps {
		if p > 0 || i == 0 {
			buf.WriteString(fmt.Sprintf("%020d", p))
		}
	}
	buf.WriteString(fmt.Sprintf(".%010d", v.d))
	return buf.String()
}

func (v Version) IsZero() bool {
	return v == VersionZero
}

func (v Version) Equal(ov Version) bool {
	return v == ov
}

func (v Version) Next(to Version) (res bool) {
	diff := uint64(0)
	for i := 0; i < VersionPartsCount; i++ {
		diff += v.ps[i] - to.ps[i]
		if diff > 1 {
			return false
		}
	}
	// if versions intergers' parts are in sequence, then just make sure the fractional parts satisfy:
	if diff == 1 {
		res = (v.d == 0 && v.d <= to.d)
		return
	}

	// valid fractional part sequence diffs:
	// 1, 10, or 10 - to.d%10
	ddiff := v.d - to.d
	switch ddiff {
	case 1, 10, (10 - to.d%10):
		ddiff = 1 // normalize fractional part result with the one of other parts
	default:
		break
	}
	res = (diff+uint64(ddiff) == 1)
	return
}

func (v Version) Before(ov Version) bool {
	for i := 0; i < VersionPartsCount; i++ {
		if v.ps[i] == ov.ps[i] {
			continue
		}
		return v.ps[i] < ov.ps[i]
	}
	return v.d < ov.d
}

func (v Version) After(ov Version) bool {
	return !v.Before(ov) && !v.Equal(ov)
}
