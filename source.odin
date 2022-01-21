package streamql

import "core:strings"

Source_Props :: enum {
	Must_Reopen,
	Is_Stdin,
}

Join_Type :: enum {
	From,
	Inner,
	Left,
	Right,
	Full,
	Cross,
}

Source_Data :: union {
	^Query,
	string,
}

Source :: struct {
	data: Source_Data,
	alias: string,
	join_type: Join_Type,
	props: bit_set[Source_Props],
}
