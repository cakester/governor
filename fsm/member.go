package fsm

var MemberUpdateDeletedType = "MEMBER_DELETED"
var MemberUpdateSetType = "MEMBER_SET"

type Member interface {
	// ID returns an unique identifier
	// For the member. This is used in
	// find and Delete operations
	ID() uint64
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

type MemberUpdate struct {
	Type string
	// The Marshalled Member structs
	OldMember     []byte
	CurrentMember []byte
}
