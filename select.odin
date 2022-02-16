//+private
package streamql

import "core:strings"

Select_Call :: proc(sel: ^Select, recs: ^Record) -> Result

Select :: struct {
	select__: Select_Call,
	schema: Schema,
	expressions: [dynamic]Expression,
	select_list: [dynamic]^Select,
	const_dest: ^Expression,
	api_ref: ^Api,
	top_count: i64,
	offset: i64,
	row_num: i64,
	rows_affected: i64,
	select_idx: i32,
}

make_select :: proc() -> Select {
	return Select {
		select__ = _select,
		expressions = make([dynamic]Expression),
		select_list = make([dynamic]^Select),
		select_idx = -1,
	}
}

select_reset :: proc(s: ^Select) -> Result {
	s.offset = 0
	s.row_num = 0
	s.rows_affected = 0

	if s.const_dest != nil {
		return not_implemented()
	}

	if len(s.select_list) != 0 {
		s.select_idx = 0
	}

	return .Ok
}

select_preop :: proc(sql: ^Streamql, s: ^Select, q: ^Query) -> Result {
	if len(s.select_list) != 0 {
		s.select_idx = 0
	}

	if s.schema.write_io == .Delimited || 
		(.Is_Default not_in s.schema.props && .Add_Header not_in sql.config) ||
		(.Is_Default in s.schema.props && .No_Header in sql.config) {
		return .Ok
	}
	return not_implemented()
}

select_connect_api :: proc(q: ^Query, api: ^Api) -> Result {
	s := &q.operation.(Select)
	if q.orderby == nil {
		s.select__ = _select_api
	} else {
		s.select__ = _select_order_api
	}
	op_expand_asterisks(q, true)

	api.fields = make([]Field, len(s.expressions))
	s.api_ref = api

	for i := 0; i < len(s.expressions); i += 1 {
		if _, is_subq := s.expressions[i].data.(Expr_Subquery); is_subq {
			return not_implemented()
		}

		//if s.expressions[i].data_type = .String {
		//	api[i]._sb = strings.make_builder()
		//}
	}

	return .Ok
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
	writer := &sel.schema.data.(Writer)
	if writer.type != nil {
		writer_set_delim(writer, sel.schema.delim)
		writer_set_rec_term(writer, sel.schema.rec_term)
	}
}

select_next_union :: proc(sel: ^Select) -> bool {
	sel.select_idx += 1
	return int(sel.select_idx) < len(sel.select_list)
}

select_verify_must_run :: proc(sel: ^Select) {
	if .Must_Run_Once in sel.schema.props {
		sel.schema.props -= {.Must_Run_Once}
		for expr in sel.expressions {
			if _, is_agg := expr.data.(Expr_Aggregate); is_agg {
				sel.schema.props += {.Must_Run_Once}
				break
			}
		}
	}
}

_select :: proc(sel: ^Select, recs: ^Record) -> Result {
	sel.row_num += 1
	w := &sel.schema.data.(Writer)

	n := w.write_record__(w, sel.expressions[:], recs) or_return

	if recs == nil {
		return .Ok
	}

	recs.offset = sel.offset
	sel.offset += i64(n)
	recs.select_len = i32(n)

	return .Ok
}

_select_api :: proc(sel: ^Select, recs: ^Record) -> Result {
	sel.row_num += 1
	res: Result

	strings.reset_builder(&sel.api_ref._sb)

	for expr, i in &sel.expressions {
		sel.api_ref.fields[i].is_null = false
		switch expr.data_type {
		case .Int:
			sel.api_ref.fields[i].data, res = expression_get_int(&expr, recs)
		case .Float:
			sel.api_ref.fields[i].data, res = expression_get_float(&expr, recs)
		case .String:
			s: string
			org_len := strings.builder_len(sel.api_ref._sb)
			s, res = expression_get_string(&expr, recs)
			strings.write_string(&sel.api_ref._sb, s)

			ret_str := strings.to_string(sel.api_ref._sb)
			sel.api_ref.fields[i].data = ret_str[org_len:len(ret_str)]
		}

		#partial switch res {
		case .Null:
			sel.api_ref.fields[i].is_null = true
		case .Ok:
		case:
			return .Error
		}
	}
	
	return .Ok
}

_select_order_api:: proc(sel: ^Select, recs: ^Record) -> Result {
	not_implemented()
	return .Error
}

_select_to_const :: proc(sel: ^Select, recs: ^Record) -> Result {
	not_implemented()
	return .Error
}

_select_to_list :: proc(sel: ^Select, recs: ^Record) -> Result {
	not_implemented()
	return .Error
}

_select_subquery :: proc(sel: ^Select, recs: ^Record) -> Result {
	not_implemented()
	return .Error
}
