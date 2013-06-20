package rethinkgo

import (
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	p "github.com/christopherhesse/rethinkgo/ql2"
	"time"
)

// connection is a connection to a rethinkdb database
type connection struct {
	// embed the net.Conn type, so that we can effectively define new methods on
	// it (interfaces do not allow that)
	net.Conn
}

var debugMode bool = false

func serverConnect(address string) (*connection, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	if err := binary.Write(conn, binary.LittleEndian, p.VersionDummy_V0_1); err != nil {
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
func (c *connection) executeQuery(queryProto *p.Query, timeout time.Duration) (result []*p.Datum, responseType p.Response_ResponseType, err error) {
	if debugMode {
		fmt.Printf("rethinkdb: queryProto:\n%v", protobufToString(queryProto, 1))
	}

	// if the user has set a timeout, make sure we set a deadline on the connection
	// so that we don't exceed the timeout.  if not, use the zero time value to
	// indicate no deadline
	if timeout == 0 {
		c.SetDeadline(time.Time{})
	} else {
		c.SetDeadline(time.Now().Add(timeout))
	}

	r, err := c.executeQueryProtobuf(queryProto)

	// reset the deadline for the connection
	c.SetDeadline(time.Time{})

	if err != nil {
		return
	}
	if debugMode {
		fmt.Printf("rethinkdb: responseProto:\n%v", protobufToString(r, 1))
	}

	responseType = r.GetType()
	switch responseType {
	case p.Response_SUCCESS_ATOM, p.Response_SUCCESS_SEQUENCE, p.Response_SUCCESS_PARTIAL:
		result = r.Response
	default:
		// some sort of error
		switch responseType {
		case p.Response_CLIENT_ERROR:
			err = ErrBrokenClient{response: r}
		case p.Response_COMPILE_ERROR:
			err = ErrBadQuery{response: r}
		case p.Response_RUNTIME_ERROR:
			err = ErrRuntime{response: r}
		default:
			err = fmt.Errorf("rethinkdb: Unexpected response type from server: %v", responseType)
		}
	}
	return
}
