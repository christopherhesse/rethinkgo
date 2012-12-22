package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"strings"
)

func protoStringOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}

func protoInt64OrNil(i int64) *int64 {
	if i == 0 {
		return nil
	}
	return proto.Int64(i)
}

func prefixLines(s string, prefix string) (result string) {
	for _, line := range strings.Split(s, "\n") {
		result += prefix + line + "\n"
	}
	return
}

func protobufToString(p proto.Message, indentLevel int) string {
	return prefixLines(proto.MarshalTextString(p), strings.Repeat("    ", indentLevel))
}
