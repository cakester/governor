package fsm

var LeaderUpdateExpiredOp = "LEADER_EXPIRED"
var LeaderUpdateChangedOp = "LEADER_CHANGED"
var LeaderUpdateDeletedOp = "LEADER_DELETED"

type Leader struct {
	Member
}

type leaderBackend struct {
	ID   uint64 `json:"id"`
	Data []byte `json:"data"`
	Time int64  `json:"time"`
	TTL  int64  `json:"ttl"`
}

type LeaderUpdate struct {
	Op string

	// The Marshalled Leader structs
	CurrentLeader []byte
	OldLeader     []byte
}
