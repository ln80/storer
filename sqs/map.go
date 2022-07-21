package sqs

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

var (
	ErrInvalidQueueMapping = errors.New("invalid queues mapping")
)

type QueueMap map[string]string

func ParseQueueMap(str string) (QueueMap, error) {
	m := make(QueueMap)

	str = strings.Trim(str, " ")
	if str == "" {
		return m, nil
	}

	for _, entry := range strings.Split(str, ";") {
		terr := fmt.Errorf("%w: %s", ErrInvalidQueueMapping, entry)

		splits := strings.Split(entry, "=")
		if len(splits) != 2 {
			return nil, terr
		}

		dest, rawUrl := strings.TrimSpace(splits[0]), strings.TrimSpace(splits[1])
		if dest == "" || rawUrl == "" {
			return nil, terr
		}
		url, err := url.ParseRequestURI(rawUrl)
		if err != nil {
			return nil, terr
		}
		m[dest] = url.String()
	}

	return m, nil
}

// WildCards returns wildcards queues names which start with '*'.
// These queues are receiving all events from the streams
func (qm QueueMap) WildCards() []string {
	m := make([]string, 0)
	for dest := range qm {
		if dest == "*" || strings.HasPrefix(dest, "*.") {
			m = append(m, dest)
		}
	}
	return m
}
