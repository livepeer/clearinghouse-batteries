// Package version holds build metadata set by release builds.
package version

var (
	Version = "dev"
	Commit  = "unknown"
)

func String() string { return Version + " (" + Commit + ")" }
