//+private
package streamql

import "core:strings"

op_get_schema :: proc(gen: ^Operation) -> ^Schema {
	gen := gen
	switch op in gen {
	case Select:
		return &op.schema
	case Update:
		return &op.schema
	case Delete:
		return &op.schema
	case Branch:
		return nil
	case Set:
		return nil
	}
	unreachable()
}

op_set_schema :: proc(gen: ^Operation, src_schema: ^Schema) {
	op_schema := op_get_schema(gen)
	schema_copy(op_schema, src_schema)
}

op_get_writer :: proc(gen: ^Operation) -> ^Writer {
	gen := gen
	switch op in gen {
	case Select:
		return &op.writer
	case Update:
		return &op.writer
	case Delete:
		return &op.writer
	case Branch:
		return nil
	case Set:
		return nil
	}
	unreachable()
}

op_set_delim :: proc(gen: ^Operation, delim: string) {
	schema_set_delim(op_get_schema(gen), delim)
}
op_set_rec_term :: proc(gen: ^Operation, rec_term: string) {
	schema_set_rec_term(op_get_schema(gen), rec_term)
}

op_set_top_count :: proc(gen: ^Operation, top_count: i64) {
	gen := gen
	#partial switch op in gen {
	case Select:
		op.top_count = top_count
	case Update:
		op.top_count = top_count
	case Delete:
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
	case Update:
		return &op.columns
	case Set:
		not_implemented()
	}
	return nil
}

op_get_additional_expressions :: proc(gen: ^Operation) -> ^[dynamic]Expression {
	up, is_update := gen.(Update)
	if is_update {
		return &up.values
	}
	return nil
}

op_writer_init :: proc(sql: ^Streamql, q: ^Query) -> Result {
	op_schema := op_get_schema(&q.operation)

	if op_schema != nil && q.union_id == 0 {
		writer := make_writer(sql, op_schema.write_io)
		op_set_writer(&q.operation, &writer)
	}

	#partial switch op in &q.operation {
	case Select:
		op_expand_asterisks(q, q.groupby == nil || q.distinct_ == nil)
		if q.distinct_ != nil {
			clear(&q.distinct_.expressions)
			for e in &op.expressions {
				new_expr := make_expression(Expr_Reference(&e))
				group_add_expression(q.distinct_, &new_expr)
			}
		}
		//select_verify_must_run(&op)
	case Update:
		return not_implemented()
	}

	return .Ok
}

op_apply_process :: proc(q: ^Query, is_subquery: bool) -> Result {
	switch op in &q.operation {
	case Select:
		select_apply_process(q, is_subquery)
		return .Ok
	case Update:
		return update_apply_process(q)
	case Delete:
		return delete_apply_process(q)
	case Branch:
		return branch_apply_process(q)
	case Set:
		return set_apply_process(q)
	}
	return .Ok
}

op_set_writer :: proc(gen: ^Operation, w: ^Writer) {
	gen := gen
	#partial switch op in gen {
	case Select:
		op.writer = w^
	case Update:
		op.writer = w^
	case Delete:
		op.writer = w^
	}
}

op_expand_asterisks :: proc(q: ^Query, force: bool) {
	op_exprs := op_get_expressions(&q.operation)
	op_schema := op_get_schema(&q.operation)

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
		if !is_subq && !force && q.sub_id == 0 && schema_eq(&q.sources[src_idx].schema, op_schema) {
			continue
		}

		_expand_asterisk(op_exprs, &q.sources[src_idx], &i)
	}

	//expression_update_indicies(&sel.expressions)

	op_writer := op_get_writer(&q.operation)
	if op_writer != nil {
		writer_resize(op_writer, len(op_exprs))
	}
}

@(private = "file")
_expand_asterisk :: proc(exprs: ^[dynamic]Expression, src: ^Source, idx: ^int) {
	aster_idx := idx^
	src_idx := i32(exprs[idx^].data.(Expr_Asterisk))

	r := &src.schema.data.(Reader)
	r.max_field_idx = i32(len(src.schema.layout) - 1)
	for item, i in src.schema.layout {
		new_expr := make_expression(item.name, "")
		expr_col := &new_expr.data.(Expr_Column_Name)
		expr_col.item.loc = item.loc
		expr_col.item.width = item.width
		expr_col.item.name = strings.clone(item.name)
		expr_col.src_idx = src_idx

		if _, is_subq := src.data.(^Query); is_subq {
			new_expr.subq_idx = u16(src_idx)
		}

		idx^ += 1
		insert_at(exprs, idx^, new_expr)
	}

	destroy_expression(&exprs[aster_idx])
	ordered_remove(exprs, aster_idx)
	idx^ -= 1
}
