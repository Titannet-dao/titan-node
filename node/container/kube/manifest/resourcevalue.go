package manifest

import "math/big"

type ResourceValue struct {
	Val *big.Int
}

func NewResourceValue(v uint64) ResourceValue {
	return ResourceValue{Val: big.NewInt(int64(v))}
}
