package fsm

import (
	"encoding/json"
	"errors"
	"github.com/compose/canoe"
	"time"
)

var forceLeaderOp = "FORCE_LEADER"
var raceLeaderOp = "RACE_LEADER"
var deleteStaleLeaderOp = "DELETE_STALE_LEADER"
var setMemberOp = "SET_MEMBER"
var deleteMemberOp = "DELETE_MEMBER"

type command struct {
	Op   string `json:"op"`
	Data []byte `json:"data"`
}

// Apply completes the FSM requirement
func (f *fsm) Apply(log canoe.LogData) error {
	var cmd command
	if err := json.Unmarshal(log, &cmd); err != nil {
		return err
	}

	switch cmd.Op {
	case forceLeaderOp:
		if err := f.applyForceLeader(cmd.Data); err != nil {
			return err
		}
	case raceLeaderOp:
		if err := f.applyRaceLeader(cmd.Data); err != nil {
			return err
		}
	case deleteStaleLeaderOp:
		if err := f.applyDeleteStaleLeader(cmd.Data); err != nil {
			return err
		}
	case setMemberOp:
		if err := f.applySetMember(cmd.Data); err != nil {
			return err
		}
	case deleteMemberOp:
		if err := f.applyDeleteMember(cmd.Data); err != nil {
			return err
		}
	default:
		return errors.New("Unknown OP")
	}
	return nil
}

type deleteStaleLeaderCmd struct {
	Time int64 `json:"time"`
}

// TODO: Send update down leader chan
func (f *fsm) applyDeleteStaleLeader(cmdData []byte) error {
	var cmd deleteStaleLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	if cmd.Time >= f.leader.Time+f.leader.TTL {
		f.leader = nil
	} else if cmd.Time < f.leader.Time {
		return ErrorBadTTLTimestamp
	}
	f.Unlock()

	return nil
}

func (f *fsm) proposeDeleteStaleLeader() error {
	req := &deleteStaleLeaderCmd{
		Time: time.Now().UnixNano(),
	}

	return f.proposeCmd(deleteStaleLeaderOp, req)
}

type forceLeaderCmd struct {
	leaderBackend
}

// TODO: Send update down leader chan
func (f *fsm) applyForceLeader(cmdData []byte) error {
	var cmd forceLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	f.leader = &cmd.leaderBackend
	f.Unlock()

	return nil
}

func (f *fsm) proposeForceLeader(leader Leader) error {
	data, err := leader.Marshal()
	if err != nil {
		return err
	}

	req := &forceLeaderCmd{
		leaderBackend{
			ID:   leader.ID(),
			Data: data,
			Time: time.Now().UnixNano(),
			TTL:  f.leaderTTL,
		},
	}

	return f.proposeCmd(forceLeaderOp, req)
}

type raceLeaderCmd struct {
	leaderBackend
}

// TODO: Send leader update down chan
func (f *fsm) applyRaceLeader(cmdData []byte) error {
	var cmd raceLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	if f.leader == nil {
		f.leader = &cmd.leaderBackend
	}
	f.Unlock()

	return nil
}

func (f *fsm) proposeRaceLeader(leader Leader) error {
	data, err := leader.Marshal()
	if err != nil {
		return err
	}

	req := &raceLeaderCmd{
		leaderBackend{
			ID:   leader.ID(),
			Data: data,
			Time: time.Now().UnixNano(),
			TTL:  f.leaderTTL,
		},
	}
	return f.proposeCmd(raceLeaderOp, req)
}

type setMemberCmd struct {
	ID   uint64 `json:"id"`
	Data []byte `json:"data"`
}

func (f *fsm) applySetMember(cmdData []byte) error {
	var cmd setMemberCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	f.members[cmd.ID] = cmd.Data
	f.Unlock()

	return nil
}

func (f *fsm) proposeSetMember(member Member) error {
	data, err := member.Marshal()
	if err != nil {
		return err
	}

	req := &setMemberCmd{
		ID:   member.ID(),
		Data: data,
	}

	return f.proposeCmd(setMemberOp, req)
}

type deleteMemberCmd struct {
	ID uint64 `json:"id"`
}

// TODO: Send update down the update chan
func (f *fsm) applyDeleteMember(cmdData []byte) error {
	var cmd deleteMemberCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	if _, ok := f.members[cmd.ID]; ok {
		delete(f.members, cmd.ID)
	}
	f.Unlock()

	return nil
}

func (f *fsm) proposeDeleteMember(id uint64) error {
	req := &deleteMemberCmd{
		ID: id,
	}

	return f.proposeCmd(deleteMemberOp, req)
}

func (f *fsm) proposeCmd(op string, data interface{}) error {
	reqData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	newCmd := &command{
		Op:   deleteStaleLeaderOp,
		Data: reqData,
	}

	newCmdData, err := json.Marshal(newCmd)
	if err != nil {
		return err
	}

	return f.raft.Propose(newCmdData)
}
