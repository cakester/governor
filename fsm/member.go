package fsm

var MemberRemovedOp = "MEMBER_REMOVED"
var MemberUpdatedOp = "MEMBER_UPDATED"
var MemberAddedOp = "MEMBER_ADDED"

type Member interface {
	// ID returns an unique identifier
	// For the member. This is used in
	// find and Delete operations
	ID() uint64
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

type MemberUpdate struct {
	Op string
	// The Marshalled Member structs
	Member []byte
}
