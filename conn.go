package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	"fmt"
	p "github.com/christopherhesse/rethinkgo/query_language"
	"net"
)

// connection is a connection to a rethinkdb database, it is not shared between
// goroutines.
type connection struct {
	net.Conn // wrap the net.Conn type, so that we can define new methods on it, effectively
}

var debugMode bool = false

const clientHello uint32 = 0xaf61ba35

func serverConnect(address string) (*connection, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	if err := binary.Write(conn, binary.LittleEndian, clientHello); err != nil {
		return nil, err
	}
	return &connection{conn}, nil
}

// SetDebug causes all queries sent to the server and responses received to be
// printed to stdout in raw form.
//
// Example usage:
//
//  r.SetDebug(true)
func SetDebug(debug bool) {
	debugMode = debug
}

// writeMessage writes a byte array to the stream preceeded by the length in
// bytes.
func (c *connection) writeMessage(data []byte) error {
	messageLength := uint32(len(data))
	if err := binary.Write(c, binary.LittleEndian, messageLength); err != nil {
		return err
	}

	_, err := c.Write(data)
	return err
}

// writeQuery writes a protobuf message to the connection.
func (c *connection) writeQuery(protobuf *p.Query) error {
	data, err := proto.Marshal(protobuf)
	if err != nil {
		return fmt.Errorf("rethinkdb: Could not marshal protocol buffer: %v, %v", protobuf, err)
	}

	return c.writeMessage(data)
}

// readMessage reads a single message from a connection.  A message is a length
// followed by a serialized protocol buffer.
func (c *connection) readMessage() ([]byte, error) {
	var messageLength uint32
	if err := binary.Read(c, binary.LittleEndian, &messageLength); err != nil {
		return nil, err
	}

	var result []byte
	buf := make([]byte, messageLength)
	for {
		n, err := c.Read(buf[0:])
		if err != nil {
			return nil, err
		}
		result = append(result, buf[0:n]...)
		if len(result) == int(messageLength) {
			break
		}
	}
	return result, nil
}

// readResponse reads a protobuf message from a connection and parses it.
func (c *connection) readResponse() (*p.Response, error) {
	data, err := c.readMessage()
	if err != nil {
		return nil, err
	}
	response := &p.Response{}
	err = proto.Unmarshal(data, response)
	return response, err
}

// executeQueryProtobuf sends a single query to the server and retrieves the parsed
// response, a lower level function used by .executeQuery()
func (c *connection) executeQueryProtobuf(protobuf *p.Query) (responseProto *p.Response, err error) {
	if err = c.writeQuery(protobuf); err != nil {
		return
	}

	for {
		responseProto, err = c.readResponse()
		if err != nil {
			return
		}

		if responseProto.GetToken() == protobuf.GetToken() {
			break
		} else if responseProto.GetToken() > protobuf.GetToken() {
			return nil, errors.New("rethinkdb: The server returned a response for a protobuf that was not submitted by us")
		}
	}
	return
}

// executeQuery is an internal function, shared by Rows iterator and the normal
// Run() call. Runs a protocol buffer formatted query, returns a list of strings
// and a status code.
func (c *connection) executeQuery(queryProto *p.Query, query Query) (result []string, status p.Response_StatusCode, err error) {
	if debugMode {
		fmt.Printf("rethinkdb: query: %v\n", query)
		fmt.Printf("rethinkdb: queryProto:\n%v", protobufToString(queryProto, 1))
	}

	r, err := c.executeQueryProtobuf(queryProto)
	if err != nil {
		return
	}
	if debugMode {
		fmt.Printf("rethinkdb: responseProto:\n%v", protobufToString(r, 1))
	}

	status = r.GetStatusCode()
	switch status {
	case p.Response_SUCCESS_JSON, p.Response_SUCCESS_STREAM, p.Response_SUCCESS_PARTIAL, p.Response_SUCCESS_EMPTY:
		// response is []string, and is empty in the case of SUCCESS_EMPTY
		result = r.Response
	default:
		// some sort of error
		e := Error{Response: r, Query: query}
		switch status {
		case p.Response_RUNTIME_ERROR:
			e.Err = ErrRuntime
		case p.Response_BAD_QUERY:
			e.Err = ErrBadQuery
		case p.Response_BROKEN_CLIENT:
			e.Err = ErrBrokenClient
		default:
			e.Err = fmt.Errorf("rethinkdb: Unexpected status code from server: %v", status)
		}
		err = e
	}
	return
}
