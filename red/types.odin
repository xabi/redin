package red

import "core:time"

RedinValue :: union {
	string,
	[dynamic]string,
	map[string]string,
	map[string]struct {},
}
RedinData :: struct {
	data:        RedinValue,
	valid_until: RedinValidityTime,
}
RedinInst :: map[string]RedinData
RedinValidityTime :: Maybe(time.Time)


BadTypeError :: struct {}
RedinError :: union {
	BadTypeError,
}

make_inst :: proc() -> ^RedinInst {
	return new(map[string]RedinData)
}
