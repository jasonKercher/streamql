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
	//schema: Schema,
	joinable_logic: []^Logic,
	//join_data: ^Hash_Join,
	join_logic: ^Logic_Group,
	join_type: Join_Type,
	props: bit_set[Source_Props],
}

construct_source_name :: proc(src: ^Source, name: string) {
	src^ = {
		data = strings.clone(name),
		//schema = make_schema(),
	}
}

construct_source_subquery :: proc(src: ^Source, subquery: ^Query) {
	src^ = {
		data = subquery,
		//schema = make_schema(),
	}
}

construct_source :: proc {
	construct_source_name,
	construct_source_subquery,
}

source_resolve_schema :: proc(sql: ^Streamql, src: ^Source) -> Result {

	delim: string

	//schema_set_delim(&src.schema, delim)
	return .Ok
}
