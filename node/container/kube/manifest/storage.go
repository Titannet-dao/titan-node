package manifest

import "strconv"

type AttributeValue interface {
	AsBool() (bool, bool)
	AsString() (string, bool)
}

type attributeValue struct {
	value string
}

func (val attributeValue) AsBool() (bool, bool) {
	if val.value == "" {
		return false, false
	}

	res, err := strconv.ParseBool(val.value)
	if err != nil {
		return false, false
	}

	return res, true
}

func (val attributeValue) AsString() (string, bool) {
	if val.value == "" {
		return "", false
	}

	return val.value, true
}

type Attributes []Attribute

func (attr Attributes) Find(glob string) AttributeValue {
	// todo wildcard

	var val attributeValue

	for i := range attr {
		if glob == attr[i].Key {
			val.value = attr[i].Value
			break
		}
	}

	return val
}

type Storage struct {
	Name       string
	Quantity   ResourceValue
	Attributes Attributes
}

func NewStorage(name string, quantity uint64, persistent bool, mount string) *Storage {
	s := &Storage{Name: name, Quantity: NewResourceValue(quantity), Attributes: make(Attributes, 0)}
	if persistent {
		attr := Attribute{Key: "persistent", Value: "true"}
		s.Attributes = append(s.Attributes, attr)
	}

	if len(mount) > 0 {
		attr := Attribute{Key: "mount", Value: mount}
		s.Attributes = append(s.Attributes, attr)
	}

	return s
}
