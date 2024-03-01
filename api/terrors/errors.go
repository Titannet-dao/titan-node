package terrors

type TError int

const (
	NotFound                 TError = iota + 10000
	DatabaseErr                     // database error
	ParametersAreWrong              // The parameters are wrong.
	CidToHashFiled                  // cid to hash failed
	UserStorageSizeNotEnough        // Insufficient user storage space
	UserNotFound                    // Unable to be found by the user
	NoDuplicateUploads              // No duplicate uploads
	BusyServer                      // Busy server
	NotFoundNode                    // Can't find the node.
	RequestNodeErr                  // Request node error
	MarshalErr                      // Marshal error

	VisitShareLinkOutOfMaxCount // visit share link out of max count
	VerifyTokenError            // verify token error
	OutOfMaxAPIKeyLimit         // out of max api key limit
	APPKeyAlreadyExist          // the APPKey already exist
	APPKeyNotFound              // the APPKey not found
	APIKeyACLError              // api key access control list error
	GroupNotEmptyCannotBeDelete // the group is not empty and cannot be deleted
	GroupNotExist               // group not exist
	GroupLimit                  // group limit
	CannotMoveToSubgroup        // cannot move to subgroup
	RootGroupCannotMoved        // the root group cannot be moved
	GroupsAreSame               // groups are the same

	NodeIPInconsistent // node ip inconsistent
	NodeDeactivate     // node deactivate
	NodeOffline        // node offline

	Success = 0
	Unknown = -1
)

func (e TError) Int() int {
	return int(e)
}

func (e TError) String() string {
	switch e {
	case UserStorageSizeNotEnough:
		return "Insufficient user storage space"
	case NotFoundNode:
		return "Can't find the node"
	default:
		return ""
	}
}
