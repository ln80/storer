package utils

import (
	"fmt"
	"strings"
)

func ParseStringToMap(str string) (m map[string]string, err error) {
	str = strings.Trim(str, " ")
	if str == "" {
		return
	}
	for _, val := range strings.Split(str, ";") {
		splits := strings.Split(val, "=")
		if len(splits) != 2 {
			err = fmt.Errorf("invalid map value %s", val)
			return
		}
		m[splits[0]] = splits[1]
	}
	return
}
