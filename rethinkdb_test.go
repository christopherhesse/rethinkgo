package rethinkgo

import (
	"fmt"
	"testing"
)

func TestArithmetic(t *testing.T) {
	SetDebug(true)

	fmt.Println("start")
	sess, err := Connect("localhost:28015", "test")
	assertOk(err)

	// TODO: query and expression have different types - right - rethinkquery interface
	// Query interface{}, Expression is a concrete type
	// can .Run() .RunCollect() .RunOne() on Query interface{} -> interface should specify that
	// can't call .Atomic() on QueryType though, still need concrete type,
	// should be public so that it can be used
	// p.Query -> queryProto
	// p.Response -> responseProto
	// Query interface{}, Expression, WriteQuery, MetaQuery struct
}

// func assertError(s *Session, q RethinkQuery) {
// 	_, err := s.Run(q)
// 	e := err.(RethinkError)
// 	fmt.Println("err:", e)
// }

func assert(condition bool) {
	if !condition {
		panic("whoops")
	}
}

func assertOk(err error) {
	if err != nil {
		panic("err:", err)
	}
}
