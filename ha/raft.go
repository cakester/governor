package ha

import (
	"encoding/bytes"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
	"github.com/coreos/etcd/stats"
	"github.com/coreos/etcd/version"
	"golang.org/x/net/context"
	"net/http"
	"sleep"
)

type raftNode struct {
	node        raft.Node
	raftStorage *raft.MemoryStorage
	transport   *rafthttp.Transport
	peers       []string

	proposeC chan string
}

func newRaftNode(addr string, peers []string, joinTimeout int) (*raftNode, error) {
	rn := &raftNode{
		proposeC:    make(chan string),
		cluster:     0x1000,
		raftStorage: raft.NewMemoryStorage(),
		peers:       peers,
		address:     addr,
		id:          Uint64UUID(),
	}

	c := &raft.Config{
		ID:              rn.id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	//rpeers, err := raftPeers(rn.peers)
	//if err != nil {
	//	return nil, err
	//}

	//rn.node = raft.StartNode(c, rpeers)

	// start prototype with the node knowing only itself
	rn.node = raft.StartNode(c, []raft.Peer{types.ID{c.ID}, addr})

	if err := rn.attachTransport(); err != nil {
		return nil, err
	}
	if err := rn.joinPeers(); err != nil {
		return nil, err
	}

	go rn.run()

	// blocking call with timeout
	// NOTE: We don't want to add ourself as our own peer yet.
	// We want some node to commit our addition to the cluster
	// so it will be written to us whenever a leader is elected
	rn.joinPeers()
	return rn, nil
}

// joinPeers forms a etcd-raft protobuf message and sends it
// round robin among known peers of another cluster
//
// We're abusing the transport here - but it allows us to
// eliminate a separate service for adding nodes to the cluster
// when we don't want another port bound up and a service to manager
func (rn *raftNode) joinPeers() error {
	confAdd := pb.ConfChange{
		Type:    pb.ConfChangeAddNode,
		NodeID:  rn.id,
		Context: rn.address,
	}

	caBytes, err := confAdd.Marshal()
	if err != nil {
		return err
	}

	confEntry := pb.Entry{
		Type: pb.EntryConfChange,
		Data: caBytes,
	}

	msg := pb.Message{
		Type:    pb.MsgProp,
		From:    rn.id,
		Entries: []Entry{confEntry},
	}

	msgBytes, err := msg.Marshal()

	client := &http.Client{}

	// send the request to all peers
	for len(rn.transport.URLs < 1) {
		for _, peer := range rn.peers {
			reader := bytes.NewReader(msgBytes)
			url := fmt.Sprintf("http://%s/%s", peer, rafthttp.RaftPrefix)
			req, err := http.NewRequest("POST", url, reader)
			if err != nil {
				return err
			}
			req.Header().Add("X-Etcd-Cluster-ID", rn.cluster)
			req.Header().Add("X-Server-Version", version.Version)
			req.Header().Add("X-Server-From", rn.id.String())

			resp, err := client.Do(req)
			if err != nil {
				return err
			}
		}
		// Wait a bit to see if changes have propagated
		// TODO: Use exponential backoff
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func (rn *raftNode) run() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		case rd := <-rc.node.Ready():
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rd.CommittedEntries); !ok {
				return
			}

			rc.node.Advance()
		}
	}
}

func (rn *raftNode) attachTransport() error {
	ss := &stats.ServerStats{}
	ss.Initialize()

	ls := &stats.LeaderStats{}

	rn.transport = &rafthttp.Transport{
		ID:          types.ID(rn.id),
		ClusterID:   0x1000,
		Raft:        rn,
		ServerStats: ss,
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rn.id)),
		ErrorC:      make(chan error),
	}

	rc.Transport.Start()
	return nil
}

func raftPeers(peers []string) []raft.Peer {
	rpeers := make([]raft.Peer, len(peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	return rpeers
}

func (rn *raftNode) Process(ctx context.Context, m raftpb.Message) {
	return rn.node.Step(ctx, m)
}
