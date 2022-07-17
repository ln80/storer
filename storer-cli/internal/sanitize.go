package internal

import (
	"bytes"
	"regexp"
	"strings"
)

func Sanitize(s string) string {
	s = strings.Trim(s, " ")

	// Flatten accents first so that if we remove non-ascii we still get a legible name
	s = accents(s)

	// Replace certain joining characters with a dash
	s = separators.ReplaceAllString(s, "-")

	// Remove all other unrecognised characters - NB we do allow any printable characters
	s = illegalPath.ReplaceAllString(s, "")

	// Remove any multiple dashes caused by replacements above
	s = dashes.ReplaceAllString(s, "-")

	return s
}

// Accents replaces a set of accented characters with ascii equivalents.
func accents(s string) string {
	// Replace some common accent characters
	b := bytes.NewBufferString("")
	for _, c := range s {
		// Check transliterations first
		if val, ok := transliterations[c]; ok {
			b.WriteString(val)
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}

var (
	illegalPath      = regexp.MustCompile(`[^[:alnum:]\~\-\./]`)
	separators       = regexp.MustCompile(`[ &_=+:]`)
	dashes           = regexp.MustCompile(`[\-]+`)
	transliterations = map[rune]string{
		'À': "A",
		'Á': "A",
		'Â': "A",
		'Ã': "A",
		'Ä': "A",
		'Å': "AA",
		'Æ': "AE",
		'Ç': "C",
		'È': "E",
		'É': "E",
		'Ê': "E",
		'Ë': "E",
		'Ì': "I",
		'Í': "I",
		'Î': "I",
		'Ï': "I",
		'Ð': "D",
		'Ł': "L",
		'Ñ': "N",
		'Ò': "O",
		'Ó': "O",
		'Ô': "O",
		'Õ': "O",
		'Ö': "OE",
		'Ø': "OE",
		'Œ': "OE",
		'Ù': "U",
		'Ú': "U",
		'Ü': "UE",
		'Û': "U",
		'Ý': "Y",
		'Þ': "TH",
		'ẞ': "SS",
		'à': "a",
		'á': "a",
		'â': "a",
		'ã': "a",
		'ä': "ae",
		'å': "aa",
		'æ': "ae",
		'ç': "c",
		'è': "e",
		'é': "e",
		'ê': "e",
		'ë': "e",
		'ì': "i",
		'í': "i",
		'î': "i",
		'ï': "i",
		'ð': "d",
		'ł': "l",
		'ñ': "n",
		'ń': "n",
		'ò': "o",
		'ó': "o",
		'ô': "o",
		'õ': "o",
		'ō': "o",
		'ö': "oe",
		'ø': "oe",
		'œ': "oe",
		'ś': "s",
		'ù': "u",
		'ú': "u",
		'û': "u",
		'ū': "u",
		'ü': "ue",
		'ý': "y",
		'ÿ': "y",
		'ż': "z",
		'þ': "th",
		'ß': "ss",
	}
)
