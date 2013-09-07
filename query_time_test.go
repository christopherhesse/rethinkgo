package rethinkgo

import (
	test "launchpad.net/gocheck"
	"time"
)

func (s *RethinkSuite) TestTime(c *test.C) {
	var response time.Time
	err := Time(1986, 11, 3, 12, 30, 15, "Z").Run(session).One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response.Equal(time.Date(1986, 11, 3, 12, 30, 15, 0, time.Local)), test.Equals, true)
}

func (s *RethinkSuite) TestEpochTime(c *test.C) {
	var response time.Time
	err := EpochTime(531360000).Run(session).One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response.Equal(time.Date(1986, 11, 3, 0, 0, 0, 0, time.Local)), test.Equals, true)
}

func (s *RethinkSuite) TestInTimezone(c *test.C) {
	loc, err := time.LoadLocation("PST8PDT")
	c.Assert(err, test.IsNil)
	var response []time.Time
	err = ExprT(List{Now(), Now().InTimezone("-07:00")}).Run(session).One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response[1].Equal(response[0].In(loc)), test.Equals, true)
}
