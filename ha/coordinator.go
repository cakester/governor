package ha

import (
	"fmt"
	"log"
	"time"
)

type coordinatorCmd struct {
	op   string
	data interface{}
}

var (
	initializeOp  = "init"
	addMemberOp   = "add_member"
	forceLeaderOp = "force_leader"
)

type Coordinator interface {
	Ping() error
	Leader() (*Leader, error)
	LeaderCh() (chan<- *Leader, error)
	TakeLeader(value string, force bool) (bool, error)
}

type CoordinatorConfig struct {
	Endpoints []string
}

type Leader struct {
	Name             string
	ConnectionString string
}
