package streamql

import "core:strings"

op_get_writer :: proc(gen: ^Operation) -> ^Writer {
	gen := gen
	switch op in gen {
	case Select:
		return &op.writer
	}
	unreachable()
}

op_set_top_count :: proc(gen: ^Operation, top_count: i64) {
	gen := gen
	#partial switch op in gen {
	case Select:
		op.top_count = top_count
	case:
		return
	}
}

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

