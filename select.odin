package streamql

import "core:strings"

Select_Call :: proc(sel: ^Select, recs: []Record) -> Result

Select :: struct {
	select__: Select_Call,
	schema: Schema,
	writer: Writer,
	expressions: [dynamic]Expression,
	const_dest: ^Expression,
	top_count: i64,
}

make_select :: proc() -> Select {
	return Select {
		expressions = make([dynamic]Expression),
	}
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
			sel.select__ = _select_order_api
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

	process = &q.plan.op_false.data
	process.props += {.Is_Passive}
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
