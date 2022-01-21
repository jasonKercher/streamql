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

op_get_expressions :: proc(gen: ^Operation) -> ^[dynamic]Expression {
	gen := gen
	#partial switch op in gen {
	case Select:
		return &op.expressions
	}
	return nil
}

op_get_additional_expressions :: proc(gen: ^Operation) -> ^[dynamic]Expression {
	return nil
}

op_writer_init :: proc(sql: ^Streamql, q: ^Query) -> Result {
	#partial switch op in &q.operation {
	case Select:
		//select_verify_must_run(&op)
	}

	return .Ok
}

op_apply_process :: proc(q: ^Query, is_subquery: bool) -> Result {
	switch op in &q.operation {
	case Select:
		//select_apply_process(q, is_subquery)
		return .Ok
	}
	return .Ok
}

op_set_writer :: proc(gen: ^Operation, w: ^Writer) {
	gen := gen
	#partial switch op in gen {
	case Select:
		op.writer = w^
	}
}

op_expand_asterisks :: proc(q: ^Query, force: bool) {
	op_exprs := op_get_expressions(&q.operation)

	for i := 0; i < len(op_exprs); i += 1 {
		aster, is_aster := op_exprs[i].data.(Expr_Asterisk)
		if !is_aster {
			continue
		}

		/* Ideally, we do not expand the asterisk. It is faster to
		 * take the whole line instead of parsing and rebuilding.
		 */
		src_idx := int(aster)
		_, is_subq := q.sources[src_idx].data.(^Query)

		_expand_asterisk(op_exprs, &q.sources[src_idx], &i)
	}

	//expression_update_indicies(&sel.expressions)

	op_writer := op_get_writer(&q.operation)
	if op_writer != nil {
		writer_resize(op_writer, len(op_exprs))
	}
}

@private
_expand_asterisk :: proc(exprs: ^[dynamic]Expression, src: ^Source, idx: ^int) {
	aster_idx := idx^
	src_idx := i32(exprs[idx^].data.(Expr_Asterisk))

	destroy_expression(&exprs[aster_idx])
	ordered_remove(exprs, aster_idx)
	idx^ -= 1
}
