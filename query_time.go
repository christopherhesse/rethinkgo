package rethinkgo

func Now() Exp {
	return Exp{kind: nowKind}
}
