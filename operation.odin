package streamql

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

	return nil
}

op_set_schema :: proc(gen: ^Operation, src_schema: ^Schema) {
	op_schema := op_get_schema(gen)
	schema_copy(op_schema, src_schema)
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
		select_expand_asterisks(q, q.groupby == nil || q.distinct_ == nil)
		if q.distinct_ != nil {
			clear(&q.distinct_.expressions)
			for e in &op.expressions {
				new_expr := make_expression(Expr_Reference(&e))
				group_add_expression(q.distinct_, &new_expr)
			}
		}
		select_verify_must_run(&op)
	case Update:
		return not_implemented()
	}

	return .Ok
}

op_apply_process :: proc(q: ^Query, is_subquery: bool) -> Result {
	switch op in &q.operation {
	case Select:
		return select_apply_process(q, is_subquery)
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
