package rethinkgo

// Returns a time object representing the current time in UTC
//
// Example usage:
//
//  var response time.Time{}
//  err = r.Now().Run(session).One(&response)
func Now() Exp {
	return nullaryOperator(nowKind)
}

// Create a time object for a specific time
//
// Example usage:
//
//  var response time.Time{}
//  err = r.Time(2006, 12, 12, 11, 30, 0, "Z").Run(session).One(&response)
func Time(year, month, day, hour, min, sec interface{}, tz string) Exp {
	return nullaryOperator(timeKind, year, month, day, hour, min, sec, tz)
}

// Returns a time object based on seconds since epoch
//
// Example usage:
//
//  var response time.Time{}
//  err = r.Now().Run(session).One(&response)
func EpochTime(epochtime interface{}) Exp {
	return nullaryOperator(epochTimeKind, epochtime)
}

// Returns a time object based on an ISO8601 formatted date-time string
//
// Example usage:
//
//  var response time.Time{}
//  err = r.Now().Run(session).One(&response)
func ISO8601(date interface{}) Exp {
	return nullaryOperator(iso8601Kind, date)
}

// Returns a new time object with a different time zone. While the time
// stays the same, the results returned by methods such as hours() will
// change since they take the timezone into account. The timezone argument
// has to be of the ISO 8601 format.
//
// Example usage:
//
//  var response time.Time{}
//  err = r.Now().InTimezone("-08:00").Hours().Run(session).One(&response)
func (e Exp) InTimezone(tz interface{}) Exp {
	return naryOperator(inTimezoneKind, e, tz)
}

// Returns the timezone of the time object
//
// Example usage:
//
//  var response time.Time{}
//  err = r.Now().Timezone().Run(session).One(&response)
func (e Exp) Timezone() Exp {
	return naryOperator(timeZoneKind, e)
}

// Returns true if a time is between two other times
// (by default, inclusive for the start, exclusive for the end).
func (e Exp) During(startTime, endTime interface{}) Exp {
	return naryOperator(duringKind, e, startTime, endTime)
}

// Return a new time object only based on the day, month and year
// (ie. the same day at 00:00).
func (e Exp) Date() Exp {
	return naryOperator(dateKind, e)
}

// Return the number of seconds elapsed since the beginning of the
// day stored in the time object.
func (e Exp) TimeOfDay() Exp {
	return naryOperator(timeOfDayKind, e)
}

// Return the year of a time object.
func (e Exp) Year() Exp {
	return naryOperator(yearKind, e)
}

// Return the month of a time object as a number between 1 and 12.
// For your convenience, the terms r.January(), r.February() etc. are
// defined and map to the appropriate integer.
func (e Exp) Month() Exp {
	return naryOperator(monthKind, e)
}

// Return the day of a time object as a number between 1 and 31.
func (e Exp) Day() Exp {
	return naryOperator(dayKind, e)
}

// Return the day of week of a time object as a number between
// 1 and 7 (following ISO 8601 standard). For your convenience,
// the terms r.Monday(), r.Tuesday() etc. are defined and map to
// the appropriate integer.
func (e Exp) DayOfWeek() Exp {
	return naryOperator(dayOfWeekKind, e)
}

// Return the day of the year of a time object as a number between
// 1 and 366 (following ISO 8601 standard).
func (e Exp) DayOfYear() Exp {
	return naryOperator(dayOfYearKind, e)
}

// Return the hour in a time object as a number between 0 and 23.
func (e Exp) Hours() Exp {
	return naryOperator(hoursKind, e)
}

// Return the minute in a time object as a number between 0 and 59.
func (e Exp) Minutes() Exp {
	return naryOperator(minutesKind, e)
}

// Return the seconds in a time object as a number between 0 and
// 59.999 (double precision).
func (e Exp) Seconds() Exp {
	return naryOperator(secondsKind, e)
}

// Convert a time object to its iso 8601 format.
func (e Exp) ToISO8601() Exp {
	return naryOperator(toIso8601Kind, e)
}

// Convert a time object to its epoch time.
func (e Exp) ToEpochTime() Exp {
	return naryOperator(toEpochTimeKind, e)
}

// Days
func Monday() Exp {
	return nullaryOperator(mondayKind)
}
func Tuesday() Exp {
	return nullaryOperator(tuesdayKind)
}
func Wednesday() Exp {
	return nullaryOperator(wednesdayKind)
}
func Thursday() Exp {
	return nullaryOperator(thursdayKind)
}
func Friday() Exp {
	return nullaryOperator(fridayKind)
}
func Saturday() Exp {
	return nullaryOperator(saturdayKind)
}
func Sunday() Exp {
	return nullaryOperator(sundayKind)
}

// Months
func January() Exp {
	return nullaryOperator(januaryKind)
}
func February() Exp {
	return nullaryOperator(februaryKind)
}
func March() Exp {
	return nullaryOperator(marchKind)
}
func April() Exp {
	return nullaryOperator(aprilKind)
}
func May() Exp {
	return nullaryOperator(mayKind)
}
func June() Exp {
	return nullaryOperator(julyKind)
}
func July() Exp {
	return nullaryOperator(julyKind)
}
func August() Exp {
	return nullaryOperator(augustKind)
}
func September() Exp {
	return nullaryOperator(septemberKind)
}
func October() Exp {
	return nullaryOperator(octoberKind)
}
func November() Exp {
	return nullaryOperator(novemberKind)
}
func December() Exp {
	return nullaryOperator(decemberKind)
}
