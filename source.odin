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
	schema: Schema,
	join_logic: ^Logic_Group,
	join_type: Join_Type,
	props: bit_set[Source_Props],
}

source_construct_name :: proc(src: ^Source, name: string) {
	src^ = {
		data = strings.clone(name),
		schema = make_schema(),
	}
}

source_constuct_subquery :: proc(src: ^Source, subquery: ^Query) {
	src^ = {
		data = subquery,
		schema = make_schema(),
	}
}

source_construct :: proc {
	source_construct_name,
	source_constuct_subquery,
}
