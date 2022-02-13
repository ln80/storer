package event

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

const (
	VersionPartsCount = 2
)

var (
	ErrVersionMaxExceeded = errors.New("version max limit exceeded")
	ErrVersionParseFailed = errors.New("failed to parse version")
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

type Version struct {
	// version parts
	ps [VersionPartsCount]uint64
	// decimal fraction
	d uint32
}

func Ver(str ...string) (Version, error) {
	if len(str) > 0 {
		return ParseVersion(str[0])
	}
	return NewVersion(), nil
}

func NewVersion() Version {
	return VersionMin
}

func TimeToVersion(t time.Time) Version {
	if t.IsZero() {
		return VersionZero
	}
	v := NewVersion()
	v.ps[0] = uint64(t.UnixNano())
	return v
}

// func VersionFromInt(i uint64) Version {
// 	v := NewVersion()
// 	v.ps[0] = uint64(i)
// 	return v
// }

func ParseVersion(vstr string) (Version, error) {
	v := Version{}

	l := len(vstr)
	if l%20 != 11 || vstr[l-11:l-10] != "." {
		return v, fmt.Errorf("%w: %s", ErrVersionParseFailed, v)
	}

	if dstr := vstr[l-10 : l]; dstr != "0000000000" {
		d, err := strconv.ParseUint(dstr, 10, 32)
		if err != nil {
			return v, err
		}
		v.d = uint32(d)
	}

	q := (l - 11) / 20
	if q > VersionPartsCount {
		return v, fmt.Errorf("%w: %s", ErrVersionMaxExceeded, v)
	}
	for i := 0; i < q; i++ {
		pstr := vstr[20*i : 20*(i+1)]
		if pstr == "00000000000000000000" {
			continue
		}
		p, err := strconv.ParseUint(pstr, 10, 64)
		if err != nil {
			return v, err
		}
		v.ps[i] = uint64(p)
	}

	return v, nil
}

func (v Version) Incr(frac ...bool) Version {
	// if len(frac) > 0 && frac[0] {
	// 	if v.d < math.MaxUint32-uint32(1) {
	// 		v.d++
	// 		return v
	// 	}
	// }

	if l := len(frac); l > 0 {
		if l == 2 && frac[0] && frac[1] {
			if v.d < math.MaxUint32-uint32(10) {
				v.d += 10
				return v
			}
		}
		if l == 1 && frac[0] {
			if v.d < math.MaxUint32-uint32(1) {
				v.d += 1
				return v
			}
		}
	}

	for i := 0; i < VersionPartsCount; i++ {
		if v.ps[i] < math.MaxUint64 {
			v.ps[i]++
			v.d = 0
			break
		} else {
			if i == VersionPartsCount {
				panic(ErrVersionMaxExceeded)
			}
		}
	}
	return v
}

func (v Version) Add(p uint64, d uint32) Version {
	if p > 0 {
		for i := 0; i < VersionPartsCount; i++ {
			if v.ps[i] < math.MaxUint64-p {
				v.ps[i] += p
				break
			} else {
				if i == VersionPartsCount {
					panic(ErrVersionMaxExceeded)
				}
			}
		}
	}
	if d > 0 {
		if v.d < math.MaxUint32-d {
			v.d += d
		} else {
			panic(ErrVersionMaxExceeded)
		}
	}
	return v
}

func (v Version) Abs() Version {
	vv := v
	vv.d = 0
	return vv
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
	defer func() {
		if r := recover(); r != nil {
			res = false
		}
	}()
	diff := uint64(0)
	for i := 0; i < VersionPartsCount; i++ {
		diff += v.ps[i] - to.ps[i]
		if diff > 1 {
			return false
		}
	}
	// valid fraction sequences incr are:
	// 1, 10, or 10 - to.d%10
	ddiff := v.d - to.d
	switch ddiff {
	case 1, 10, (10 - to.d%10):
		ddiff = 1 // normalize decimal diff with diffs of other parts
	default:
		break
	}
	diff += uint64(ddiff)
	res = diff == 1
	return
}

func (v Version) Before(ov Version) bool {
	for i := 0; i < VersionPartsCount; i++ {
		if v.ps[i] < ov.ps[i] {
			return true
		}
	}
	return v.d < ov.d
}

func (v Version) After(ov Version) bool {
	return !v.Before(ov) && !v.Equal(ov)
}
