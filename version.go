package storer

import (
	"github.com/Masterminds/semver/v3"
)

type Version string

// VERSION is the current version of the Storer Go Module
const VERSION Version = "v0.1.1"

// Semver parses and returns semver struct.
func (v Version) Semver() *semver.Version {
	return semver.MustParse(string(v))
}
