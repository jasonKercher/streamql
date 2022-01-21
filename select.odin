package streamql

import "core:strings"

Select_Call :: proc(sel: ^Select) -> Result

Select :: struct {
	select__: Select_Call,
	writer: Writer,
	top_count: i64,
}

make_select :: proc() -> Select {
	return Select {
		top_count = 0,
	}
}

select_apply_process :: proc(q: ^Query, is_subquery: bool) {
	sel := &q.operation.(Select)
	process := &q.plan.op_true.data
	process.action__ = sql_select
	process.data = sel

	/* Build plan description */
	b := strings.make_builder()
	strings.write_string(&b, "SELECT ")

	first := true
	process = &q.plan.op_false.data
	process.props += {.Is_Passive}
}

_select_to_const :: proc(sel: ^Select) -> Result {
	return not_implemented()
}

_select_order_api:: proc(sel: ^Select) -> Result {
	return not_implemented()
}

_select_subquery :: proc(sel: ^Select) -> Result {
	return not_implemented()
}
