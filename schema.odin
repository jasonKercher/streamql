package streamql

Schema_Props :: enum {
	Is_Var,
}

Schema :: struct {
	name: string,
	props: bit_set[Schema_Props],
}

make_schema :: proc() -> Schema {
	return Schema {}
}
