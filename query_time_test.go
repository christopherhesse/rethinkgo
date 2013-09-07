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

func (s *RethinkSuite) TestISO8601(c *test.C) {
	var t1, t2 time.Time
	t2, _ = time.Parse("2006-01-02T15:04:05-07:00", "1986-11-03T08:30:00-07:00")
	err := ISO8601("1986-11-03T08:30:00-07:00").Run(session).One(&t1)
	c.Assert(err, test.IsNil)
	c.Assert(t1.Equal(t2), test.Equals, true)
}

func (s *RethinkSuite) TestInTimezone(c *test.C) {
	loc, err := time.LoadLocation("MST")
	c.Assert(err, test.IsNil)
	var response []time.Time
	err = Expr(List{Now(), Now().InTimezone("-07:00")}).Run(session).One(&response)
	c.Assert(err, test.IsNil)
	c.Assert(response[1].Equal(response[0].In(loc)), test.Equals, true)
}

func (s *RethinkSuite) TestBetween(c *test.C) {
	var response interface{}

	times := Expr(List{
		Time(1986, 9, 3, 12, 30, 15, "Z"),
		Time(1986, 10, 3, 12, 30, 15, "Z"),
		Time(1986, 11, 3, 12, 30, 15, "Z"),
		Time(1986, 12, 3, 12, 30, 15, "Z"),
	})
	err := times.Filter(func(row Exp) Exp {
		return row.During(Time(1986, 9, 3, 12, 30, 15, "Z"), Time(1986, 11, 3, 12, 30, 15, "Z"))
	}).Count().Run(session).One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(int(response.(float64)), test.Equals, 2)
}

func (s *RethinkSuite) TestYear(c *test.C) {
	var response interface{}

	err := Time(1986, 12, 3, 12, 30, 15, "Z").Year().Run(session).One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(int(response.(float64)), test.Equals, 1986)
}

func (s *RethinkSuite) TestMonth(c *test.C) {
	var response interface{}

	err := Time(1986, 12, 3, 12, 30, 15, "Z").Month().Eq(December()).Run(session).One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response.(bool), test.Equals, true)
}

func (s *RethinkSuite) TestDay(c *test.C) {
	var response interface{}

	err := Time(1986, 12, 3, 12, 30, 15, "Z").Day().Eq(Wednesday()).Run(session).One(&response)

	c.Assert(err, test.IsNil)
	c.Assert(response.(bool), test.Equals, true)
}
