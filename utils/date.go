package utils

import "time"

func FromUTCToTimezone(utcTime time.Time, timezone string) time.Time {
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return utcTime
	}
	return utcTime.In(loc)
}

func TimeToTimestamp(t time.Time) int64 {
	return t.Unix()
}
