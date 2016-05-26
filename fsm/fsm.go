package fsm

import (
	"encoding/json"
	"errors"
	"github.com/compose/canoe"
	"github.com/gorilla/mux"
	"reflect"
	"sync"
	"time"
)

type fsm struct {
	sync.Mutex

	syncTicker <-chan time.Time

	raft *canoe.Node

	leader    *leaderBackend
	leaderc   chan *LeaderUpdate
	leaderTTL int64

	members map[uint64][]byte
	memberc chan *MemberUpdate

	stopc    chan struct{}
	stoppedc chan struct{}
}

type Config struct {
	RaftPort       int
	APIPort        int
	BootstrapPeers []string
	BootstrapNode  bool
	DataDir        string
	ClusterID      uint64
	LeaderTTL      int64
}

func NewGovernorFSM(config *Config) (*fsm, error) {
	newFSM := &fsm{
		leaderTTL:  config.LeaderTTL,
		members:    make(map[uint64][]byte),
		syncTicker: time.Tick(500 * time.Millisecond),
		stopc:      make(chan struct{}),
		stoppedc:   make(chan struct{}),
	}

	raftConfig := &canoe.NodeConfig{
		FSM:            newFSM,
		ClusterID:      config.ClusterID,
		RaftPort:       config.RaftPort,
		APIPort:        config.APIPort,
		BootstrapPeers: config.BootstrapPeers,
		DataDir:        config.DataDir,
		SnapshotConfig: &canoe.SnapshotConfig{
			Interval: 20 * time.Second,
		},
	}

	node, err := canoe.NewNode(raftConfig)
	if err != nil {
		return nil, err
	}

	newFSM.raft = node

	if err := newFSM.start(); err != nil {
		return nil, err
	}

	return newFSM, nil
}

func (f *fsm) start() error {
	if err := f.raft.Start(); err != nil {
		return err
	}

	go func(f *fsm) {
		if err := f.run(); err != nil {
			panic(err)
		}
	}(f)

	return nil
}

func (f *fsm) run() error {
	defer func(f *fsm) {
		close(f.stoppedc)
	}(f)

	ttlTicker := time.NewTicker(500 * time.Millisecond)

	for {
		select {
		case <-ttlTicker.C:
			if err := f.proposeDeleteStaleLeader(); err != nil {
				return err
			}
		}
	}
	return nil
}

// LeaderCh returns a channel with LeaderUpdates
// LeaderCh does not block. Note: this means if the user is not monitoring
// LeaderCh then the LeaderUpdate will be lost it is the user's
// responsibility to ensure the channel is consumed as aggressively as is needed
// based on expected update to the leader
func (f *fsm) LeaderCh() <-chan *LeaderUpdate {
	return f.leaderc
}

func (f *fsm) RaceForLeader(leader Leader) error {
	return f.proposeRaceLeader(leader)
}

func (f *fsm) ForceLeader(leader Leader) error {
	return f.proposeForceLeader(leader)
}

func (f *fsm) DeleteLeader() error {
	return f.proposeDeleteLeader()
}

func (f *fsm) Leader(leader Leader) error {
	f.Lock()
	defer f.Unlock()
	if f.leader == nil {
		f.leader = nil
		return nil
	}

	return leader.Unmarshal(f.leader.Data)
}

func (f *fsm) MemberCh() <-chan *MemberUpdate {
	return f.memberc
}

func (f *fsm) SetMember(member Member) error {
	return f.proposeSetMember(member)
}

func (f *fsm) DeleteMember(id uint64) error {
	return f.proposeDeleteMember(id)
}

func (f *fsm) Member(id uint64, member Member) (bool, error) {
	f.Lock()
	defer f.Unlock()
	if data, ok := f.members[id]; ok {
		err := member.Unmarshal(data)
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// Members gives all the members of the cluster
// you must pass a pointer to a slice of
func (f *fsm) Members(members interface{}) error {
	// Documented here http://stackoverflow.com/questions/25384640/why-golang-reflect-makeslice-returns-un-addressable-value
	// And the example from mgo http://bazaar.launchpad.net/+branch/mgo/v2/view/head:/session.go#L2769
	// This explains the odd reason for specifying the pointer to slice
	resultv := reflect.ValueOf(members)
	memberType := reflect.TypeOf((*Member)(nil)).Elem()

	if resultv.Kind() != reflect.Ptr ||
		resultv.Elem().Kind() != reflect.Slice ||
		!reflect.PtrTo(resultv.Elem().Type().Elem()).Implements(memberType) {

		return errors.New("Must provide a pointer to slice of Member - &[]Member")
	}

	sliceType := resultv.Elem().Type().Elem()
	retMembers := reflect.Indirect(reflect.New(resultv.Elem().Type()))

	f.Lock()
	defer f.Unlock()
	for _, memberBackend := range f.members {
		member := reflect.New(sliceType).Interface().(Member)
		if err := member.Unmarshal(memberBackend); err != nil {
			return err
		}

		retMembers.Set(
			reflect.Append(
				reflect.Indirect(retMembers),
				reflect.Indirect(reflect.ValueOf(member)),
			),
		)
	}

	resultv.Elem().Set(retMembers)

	return nil
}

func (f *fsm) Cleanup() error {
	if err := f.raft.Stop(); err != nil {
		return err
	}
	close(f.stopc)

	select {
	case <-f.stoppedc:
	case <-time.Tick(10 * time.Second):
		return ErrorTimedOutCleanup
	}

	return nil
}

func (f *fsm) Destroy() error {
	if err := f.raft.Destroy(); err != nil {
		return err
	}

	close(f.stopc)

	select {
	case <-f.stoppedc:
	case <-time.Tick(10 * time.Second):
		return ErrorTimedOutCleanup
	}

	return nil
	close(f.stopc)

	select {
	case <-f.stoppedc:
	case <-time.Tick(10 * time.Second):
		return ErrorTimedOutDestroy
	}

	return nil
}

type fsmSnapshot struct {
	Members map[uint64][]byte `json:"members"`
	Leader  *leaderBackend    `json:"leader"`
}

func (f *fsm) Restore(data canoe.SnapshotData) error {
	var fsmSnap fsmSnapshot

	if err := json.Unmarshal(data, &fsmSnap); err != nil {
		return err
	}

	f.Lock()
	defer f.Unlock()
	// Don't worry with chan notifications here
	// As snapshots are only applied at startup
	f.members = fsmSnap.Members
	f.leader = fsmSnap.Leader

	return nil
}

func (f *fsm) Snapshot() (canoe.SnapshotData, error) {
	f.Lock()
	defer f.Unlock()
	return json.Marshal(&fsmSnapshot{
		Members: f.members,
		Leader:  f.leader,
	})
}

func (f *fsm) RegisterAPI(router *mux.Router) {
	return
}
