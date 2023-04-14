package api

import (
	"github.com/filecoin-project/go-jsonrpc/auth"
)

const (
	// When changing these, update docs/API.md too

	PermRead  auth.Permission = "read" // default
	PermWrite auth.Permission = "write"
	PermSign  auth.Permission = "sign"  // Use wallet keys for signing
	PermAdmin auth.Permission = "admin" // Manage permissions
)

var AllPermissions = []auth.Permission{PermRead, PermWrite, PermSign, PermAdmin}
var DefaultPerms = []auth.Permission{PermRead}
var ReadWritePerms = []auth.Permission{PermRead, PermWrite}

func permissionedProxies(in, out interface{}) {
	outs := GetInternalStructs(out)
	for _, o := range outs {
		auth.PermissionedProxy(AllPermissions, DefaultPerms, in, o)
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
