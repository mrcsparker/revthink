package revthink

import (
	"encoding/json"
	"errors"
	"fmt"
	r "github.com/dancannon/gorethink"
	"github.com/revel/revel"
	"io/ioutil"
)

var (
	Session *r.Session // global RethinkDB session
	Address string     // global RethinkDB address
)

func InitController() {
	revel.InterceptMethod((*RethinkDBController).Begin, revel.BEFORE)
	revel.InterceptMethod((*RethinkDBController).End, revel.FINALLY)
}

func InitDB() {

	configRequired := func(key string) string {
		value, found := revel.Config.String(key)
		if !found {
			revel.ERROR.Fatal(fmt.Sprintf("Configuration for %s missing in app.conf.", key))
		}
		return value
	}

	Address = configRequired("database.address")

	connect()

	revel.INFO.Println("Connected to RethinkDB:", Session.IsConnected())

}

type RethinkDBController struct {
	*revel.Controller
	RethinkDBSession *r.Session
}

func GetSession() (*r.Session, error) {
	if Session == nil || !Session.IsConnected() {
		var err error

		Session, err = r.Connect(r.ConnectOpts{
			Address: Address,
		})

		if err != nil {
			return nil, err
		}

		if !Session.IsConnected() {
			return nil, errors.New("Not connected to RethinkDB")
		}

		Session.SetMaxOpenConns(5)
	}
	return Session, nil
}

func connect() {
	var err error
	Session, err = GetSession()

	if err != nil {
		revel.ERROR.Println("FATAL", err.Error())
		panic(err.Error())
	}
}

func (c *RethinkDBController) Begin() revel.Result {
	revel.INFO.Println("Setting up RethinkDBSession")

	if Session == nil || !Session.IsConnected() {
		revel.INFO.Println("Reconnecting to RethinkDBSession")
		connect()
	}

	c.RethinkDBSession = Session
	return nil
}

func (c *RethinkDBController) End() revel.Result {
	revel.INFO.Println("Cleaning up RethinkDBSession")
	if c.RethinkDBSession != nil && c.RethinkDBSession.IsConnected() {
		c.RethinkDBSession.Close()
	}
	return nil
}
