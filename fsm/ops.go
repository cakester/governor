package fsm

import (
	"encoding/json"
	"github.com/compose/canoe"
	"time"
)

var forceLeaderOp = "FORCE_LEADER"
var deleteLeaderOp = "DELETE_LEADER"
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
	case deleteLeaderOp:
		if err := f.applyDeleteLeader(); err != nil {
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
		return ErrorUnknownOperation
	}
	return nil
}

type deleteLeaderCmd struct {
}

func (f *fsm) applyDeleteLeader() error {
	update := &LeaderUpdate{
		Type: LeaderUpdateDeletedType,
	}

	f.Lock()
	defer f.Unlock()

	if f.leader != nil {
		update.OldLeader = f.leader.Data
	}

	f.leader = nil

	select {
	case f.leaderc <- update:
	default:
	}

	return nil
}

func (f *fsm) proposeDeleteLeader() error {
	req := &deleteLeaderCmd{}

	return f.proposeCmd(deleteStaleLeaderOp, req)
}

type deleteStaleLeaderCmd struct {
	Time int64 `json:"time"`
}

func (f *fsm) applyDeleteStaleLeader(cmdData []byte) error {
	var cmd deleteStaleLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	update := &LeaderUpdate{
		Type: LeaderUpdateDeletedType,
	}

	f.Lock()
	defer f.Unlock()

	if f.leader != nil {
		update.OldLeader = f.leader.Data
	}

	if cmd.Time >= f.leader.Time+f.leader.TTL {
		f.leader = nil
	} else if cmd.Time < f.leader.Time {
		return ErrorBadTTLTimestamp
	}

	select {
	case f.leaderc <- update:
	default:
	}
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

func (f *fsm) applyForceLeader(cmdData []byte) error {
	var cmd forceLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	update := &LeaderUpdate{
		Type: LeaderUpdateSetType,
	}

	f.Lock()
	defer f.Unlock()

	if f.leader != nil {
		update.OldLeader = f.leader.Data
	}

	f.leader = &cmd.leaderBackend

	update.CurrentLeader = f.leader.Data

	select {
	case f.leaderc <- update:
	default:
	}

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

func (f *fsm) applyRaceLeader(cmdData []byte) error {
	var cmd raceLeaderCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()
	if f.leader == nil {
		f.leader = &cmd.leaderBackend

		update := &LeaderUpdate{
			Type:          LeaderUpdateSetType,
			CurrentLeader: f.leader.Data,
		}

		select {
		case f.leaderc <- update:
		default:
		}
	}

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
	defer f.Unlock()
	update := &MemberUpdate{
		Type:      MemberUpdateSetType,
		OldMember: f.members[cmd.ID],
	}
	f.members[cmd.ID] = cmd.Data

	update.CurrentMember = f.members[cmd.ID]

	select {
	case f.memberc <- update:
	default:
	}

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

func (f *fsm) applyDeleteMember(cmdData []byte) error {
	var cmd deleteMemberCmd
	if err := json.Unmarshal(cmdData, &cmd); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()
	if _, ok := f.members[cmd.ID]; ok {
		update := &MemberUpdate{
			Type:      MemberUpdateDeletedType,
			OldMember: f.members[cmd.ID],
		}

		select {
		case f.memberc <- update:
		default:
		}
		delete(f.members, cmd.ID)
	}

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
