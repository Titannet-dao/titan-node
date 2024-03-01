package api

import (
	"context"
	"reflect"
	"strings"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/filecoin-project/go-jsonrpc/auth"
	xerrors "golang.org/x/xerrors"
)

type permKey int
type userAccessControlKey struct{}

var permCtxKey permKey
var aclCtxKey userAccessControlKey

func split(psStr auth.Permission) []auth.Permission {
	permissions := strings.Split(string(psStr), ",")
	ps := []auth.Permission{}
	for _, permission := range permissions {
		p := auth.Permission(permission)
		ps = append(ps, p)
	}

	return ps
}

func WithPerm(ctx context.Context, perms []auth.Permission) context.Context {
	return context.WithValue(ctx, permCtxKey, perms)
}

func WithUserAccessControl(ctx context.Context, acl []types.UserAccessControl) context.Context {
	return context.WithValue(ctx, aclCtxKey, acl)
}

func HasPerm(ctx context.Context, defaultPerm auth.Permission, perms auth.Permission) bool {
	callerPerms, ok := ctx.Value(permCtxKey).([]auth.Permission)
	if !ok {
		callerPerms = append(callerPerms, defaultPerm)
	}

	for _, callerPerm := range callerPerms {
		ps := split(perms)
		for _, p := range ps {
			if p == defaultPerm {
				return true
			}
			if p == callerPerm {
				return true
			}
		}
	}
	return false
}

func PermissionedProxy(validPerms []auth.Permission, defaultPerms auth.Permission, in interface{}, out interface{}) {
	rint := reflect.ValueOf(out).Elem()
	ra := reflect.ValueOf(in)

	for f := 0; f < rint.NumField(); f++ {
		field := rint.Type().Field(f)
		requiredPerms := auth.Permission(field.Tag.Get("perm"))
		if requiredPerms == "" {
			panic("missing 'perm' tag on " + field.Name) // ok
		}

		// Validate perm tag
		ok := false
	exit:
		for _, perm := range validPerms {
			ps := split(requiredPerms)
			for _, p := range ps {
				if p == perm {
					ok = true
					break exit
				}
			}
		}

		if !ok {
			panic("unknown 'perm' tag on " + field.Name) // ok
		}

		fn := ra.MethodByName(field.Name)

		rint.Field(f).Set(reflect.MakeFunc(field.Type, func(args []reflect.Value) (results []reflect.Value) {
			err := xerrors.Errorf("missing permission to invoke '%s' (need '%s')", field.Name, requiredPerms)

			ctx := args[0].Interface().(context.Context)
			if HasPerm(ctx, defaultPerms, requiredPerms) {
				if AllowUserAccess(ctx, requiredPerms, field.Name) {
					return fn.Call(args)
				}

				err = xerrors.Errorf("user missing access control: %s, own: %s", types.FuncAccessControlMap[field.Name], ctx.Value(userAccessControlKey{}).([]types.UserAccessControl))
			}

			rerr := reflect.ValueOf(&err).Elem()

			if field.Type.NumOut() == 2 {
				return []reflect.Value{
					reflect.Zero(field.Type.Out(0)),
					rerr,
				}
			} else {
				return []reflect.Value{rerr}
			}
		}))

	}
}

func AllowUserAccess(ctx context.Context, perms auth.Permission, funcName string) bool {
	if !isNeedUserAccessControl(ctx, perms) {
		return true
	}

	accessControlList, ok := ctx.Value(userAccessControlKey{}).([]types.UserAccessControl)
	if !ok {
		panic("can not get user accessControl from context")
	}

	needPermission, ok := types.FuncAccessControlMap[funcName]
	if !ok {
		return true
	}

	for _, ac := range accessControlList {
		if ac == needPermission {
			return true
		}
	}
	return false

}

func isNeedUserAccessControl(ctx context.Context, perms auth.Permission) bool {
	callerPerms, ok := ctx.Value(permCtxKey).([]auth.Permission)
	if !ok {
		return false
	}

	isNeedAccessControl := false
	for _, perm := range callerPerms {
		if perm == RoleAdmin {
			return false
		}

		if perm == RoleUser {
			isNeedAccessControl = true
		}
	}

	if !isNeedAccessControl {
		return false
	}

	ps := split(perms)
	for _, p := range ps {
		if p == RoleUser {
			return true
		}
	}

	return false
}
