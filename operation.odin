package streamql

import "core:strings"

op_writer_init :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return .Ok
}

op_apply_process :: proc(q: ^Query, is_subquery: bool) -> Result {
	return .Ok
}

op_set_writer :: proc(gen: ^Operation, w: ^Writer) {
	gen := gen
	#partial switch op in gen {
	case Select:
		op.writer = w^
	}
}

