package tunnel

import "strings"

const netErrCloseByRemoteHost = "closed by the remote host"
const netErrConnectionResetByPeer = "connection reset by peer"
const netErrUseOfCloseNetworkConnection = "use of closed network connection"

func isNetErrCloseByRemoteHost(err error) bool {
	if strings.Contains(err.Error(), netErrCloseByRemoteHost) {
		return true
	}
	return false
}

func isNetErrConnectionResetByPeer(err error) bool {
	if strings.Contains(err.Error(), netErrConnectionResetByPeer) {
		return true
	}
	return false
}

func isNetErrUseOfCloseNetworkConnection(err error) bool {
	if strings.Contains(err.Error(), netErrUseOfCloseNetworkConnection) {
		return true
	}
	return false
}
