//+private
package streamql

import "core:strings"

Select_Call :: proc(sel: ^Select, recs: []Record) -> Result

Select :: struct {
	select__: Select_Call,
	schema: Schema,
	writer: Writer,
	curr: ^Select,
	expressions: [dynamic]Expression,
	union_selects: [dynamic]^Select,
	const_dest: ^Expression,
	top_count: i64,
	offset: i64,
	row_num: i64,
	rows_affected: i64,
	union_idx: i32,
}

make_select :: proc() -> Select {
	return Select {
		expressions = make([dynamic]Expression),
		union_idx = -1,
	}
}

select_reset :: proc(s: ^Select) -> Result {
	s.offset = 0
	s.row_num = 0
	s.rows_affected = 0

	if s.const_dest != nil {
		return not_implemented()
	}

	if len(s.union_selects) != 0 {
		s.union_idx = 0
	}
	s.curr = s

	return .Ok
}

select_preop :: proc(sql: ^Streamql, s: ^Select, q: ^Query) -> Result {
	if len(s.union_selects) != 0 {
		s.union_idx = 0
	}

	if s.schema.write_io == .Delimited || 
		(.Is_Default not_in s.schema.props && .Add_Header not_in sql.config) ||
		(.Is_Default in s.schema.props && .No_Header in sql.config) {
		return .Ok
	}
	return not_implemented()
}

select_add_expression :: proc(s: ^Select, expr: ^Expression) -> ^Expression {
	append(&s.expressions, expr^)
	return &s.expressions[len(s.expressions) - 1]
}

select_apply_alias :: proc(s: ^Select, alias: string) {
	expr := &s.expressions[len(s.expressions) - 1]
	expr.alias = strings.clone(alias)
}

select_resolve_type_from_subquery :: proc(expr: ^Expression) -> Result {
	return not_implemented()
}

select_apply_process :: proc(q: ^Query, is_subquery: bool) {
	sel := &q.operation.(Select)
	process := &q.plan.op_true.data
	process.action__ = sql_select
	process.data = sel

	if sel.const_dest != nil {
		if q.orderby != nil {
			sel.select__ = _select_to_const
		} else {
			sel.select__ = _select_order_api
		}
	} else if is_subquery {
		sel.select__ = _select_subquery
	}

	/* Build plan description */
	b := strings.make_builder()
	strings.write_string(&b, "SELECT ")

	first := true
	for e in &sel.expressions {
		if !first {
			strings.write_byte(&b, ',')
		}
		first = false
		expression_cat_description(&e, &b)
	}

	process.msg = strings.to_string(b)

	process = &q.plan.op_false.data
	process.state += {.Is_Passive}
	if sel.writer.type != nil {
		writer_set_delim(&sel.writer, sel.schema.delim)
		writer_set_rec_term(&sel.writer, sel.schema.rec_term)
	}
}

_select_to_const :: proc(sel: ^Select, recs: []Record) -> Result {
	return not_implemented()
}

_select_order_api:: proc(sel: ^Select, recs: []Record) -> Result {
	return not_implemented()
}

_select_subquery :: proc(sel: ^Select, recs: []Record) -> Result {
	return not_implemented()
}
