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

func (e Exp) InTimezone(tz string) Exp {
	return naryOperator(inTimezoneKind, e, tz)
}
