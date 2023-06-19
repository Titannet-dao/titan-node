package api

import (
	"github.com/filecoin-project/go-jsonrpc/auth"
)

const (
	// When changing these, update docs/API.md too

	RoleWeb       auth.Permission = "web"
	RoleCandidate auth.Permission = "candidate"
	RoleEdge      auth.Permission = "edge"
	RoleLocator   auth.Permission = "locator"
	RoleAdmin     auth.Permission = "admin" // Manage permissions
	RoleDefault   auth.Permission = "default"
	RoleUser      auth.Permission = "user"
)

var AllPermissions = []auth.Permission{RoleWeb, RoleCandidate, RoleEdge, RoleLocator, RoleAdmin, RoleDefault, RoleUser}

func permissionedProxies(in, out interface{}) {
	outs := GetInternalStructs(out)
	for _, o := range outs {
		PermissionedProxy(AllPermissions, RoleDefault, in, o)
	}
}

func PermissionedCandidateAPI(a Candidate) Candidate {
	var out CandidateStruct
	permissionedProxies(a, &out)
	return &out
}

func PermissionedSchedulerAPI(a Scheduler) Scheduler {
	var out SchedulerStruct
	permissionedProxies(a, &out)
	return &out
}

func PermissionedEdgeAPI(a Edge) Edge {
	var out EdgeStruct
	permissionedProxies(a, &out)
	return &out
}

func PermissionedLocationAPI(a Locator) Locator {
	var out LocatorStruct
	permissionedProxies(a, &out)
	return &out
}
