package terrors

const (
	NotFound                 = iota + 10000
	DatabaseErr              // database error
	ParametersAreWrong       // The parameters are wrong.
	CidToHashFiled           // cid to hash failed
	UserStorageSizeNotEnough // Insufficient user storage space
	UserNotFound             // Unable to be found by the user
	NoDuplicateUploads       // No duplicate uploads
	BusyServer               // Busy server
	NotFoundNode             // Can't find the node.
	RequestNodeErr           // Request node error
	MarshalErr               // Marshal error

	VisitShareLinkOutOfMaxCount //visit share link out of max count
	VerifyTokenError            // verify token error

	Success = 0
	Unknown = -1
)
