package streamql

import "core:os"
import "core:fmt"
import "core:math/bits"

Operation :: union {
	Set,
	Branch,
	Select,
	Update,
	Delete,
}

Query :: struct {
	operation: Operation,
	plan: ^Plan,
	sources: [dynamic]Source,
	groupby: ^Group,
	distinct_: ^Group,
	where_: ^Logic_Group,
	having: ^Logic_Group,
	orderby: ^Order,
	unions: [dynamic]^Query,
	subquery_exprs: [dynamic]^Query,
	var_source_vars: [dynamic]i32,
	var_sources: [dynamic]i32,
	var_expr_vars: [dynamic]i32,
	var_exprs: [dynamic]i32,
	into_table_name: string,
	preview_text: string,
	top_count: u64,
	top_expr: ^Expression,
	idx: u32,
	into_table_var: i32,
	next_idx: i32,
	union_id: i32,
	sub_id: i16,
	query_total: i16,
}

/* All dynamic fields are initialized as needed.
 * Also note that queries only exist as pointers.
 */
new_query :: proc(sub_id: i16) -> ^Query {
	q := new(Query)
	q = {
		expressions = make([dynamic]Expression),
		top_count = bits.U64_MAX,
		into_table_var = -1,
		sub_id = sub_id,
	}
	return q
}

@(private = "file")
_distribute_expression :: proc(sql: ^Streamql, q: ^Query, expr: ^Expression) -> Result {
	if len(sql.function_stack) > 0 {
		fn_expr := sql.function_stack[len(sql.function_stack) - 1]
		function_add_expression(fn_expr, expr)
		return .Ok
	}
	switch sql.listener.mode {
	case .Select_List:
		select_add_expression(&q.operation.(Select), expr)
	case .Update_List:
		return update_add_expression(&q.operation.(Update), expr)
	case .Top:
		self.top_expr = new(Expression)
		self.top_expr^ = expr^
	case .Case:
		return .Error
	case .In:
		fallthrough
	case .Logic:
		return _add_logic_expression(sql, q, expr)
	case .Groupby:
		group_add_expression(q.groupby, expr)
	case .Orderby:
		order_add_expression(q.orderby, expr)
	case .Set:
		fallthrough
	case .Declare:
		set_set_init_expression(q.operation.(Set), expr)
	}

	return .Ok
}

@(private = "file")
_add_logic_expression :: proc(sql: ^Streamql, q: ^Query, expr: ^Expression) -> Result {
	if sql.listener.logic_mode != .Having && expr.type == .Aggregate {
		fmt.fprintln(os.stderr, "cannot have aggregate logic outside of HAVING")
		return .Error
	}

	lg := &sql.logic_stack[len(sql.logic_stack) - 1]
	if lg.condition == nil {
		lg.condition = new_logic()
	}
	logic_add_expression(lg.condition, expr)

	return .Ok
}
