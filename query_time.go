package rethinkgo

func Now() Exp {
	return nullaryOperator(nowKind)
}

func Time(year, month, day, hour, min, sec int, tz string) Exp {
	return nullaryOperator(timeKind, year, month, day, hour, min, sec, tz)
}

func EpochTime(epochtime int) Exp {
	return nullaryOperator(epochTimeKind, epochtime)
}

func ISO8601(date string) Exp {
	return nullaryOperator(iso8601Kind, date)
}

func (e Exp) InTimezone(tz string) Exp {
	return naryOperator(inTimezoneKind, e, tz)
}

func (e Exp) Timezone() Exp {
	return naryOperator(timeZoneKind, e)
}

func (e Exp) During(startTime, endTime interface{}) Exp {
	return naryOperator(duringKind, e, startTime, endTime)
}

func (e Exp) Date() Exp {
	return naryOperator(dateKind, e)
}

func (e Exp) TimeOfDay() Exp {
	return naryOperator(timeOfDayKind, e)
}

func (e Exp) Year() Exp {
	return naryOperator(yearKind, e)
}

func (e Exp) Month() Exp {
	return naryOperator(monthKind, e)
}

func (e Exp) Day() Exp {
	return naryOperator(dayKind, e)
}

func (e Exp) DayOfWeek() Exp {
	return naryOperator(dayOfWeekKind, e)
}

func (e Exp) DayOfYear() Exp {
	return naryOperator(dayOfYearKind, e)
}

func (e Exp) Hours() Exp {
	return naryOperator(hoursKind, e)
}

func (e Exp) Minutes() Exp {
	return naryOperator(minutesKind, e)
}

func (e Exp) Seconds() Exp {
	return naryOperator(secondsKind, e)
}

func (e Exp) ToISO8601() Exp {
	return naryOperator(toIso8601Kind, e)
}

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
