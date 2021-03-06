//+private
package streamql

import "util"
import "core:os"
import "core:fmt"
import "core:strings"

Operation :: union {
	Set,
	Branch,
	Select,
	Update,
	Delete,
}

Query :: struct {
	operation: Operation,
	plan: Plan,
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
	var_exprs: [dynamic]^Expression,
	joinable_logic: [dynamic]^Logic,
	into_table_name: string,
	preview_text: string,
	state: ^Listener_State,
	top_count: i64,
	top_expr: ^Expression,
	next_idx_ref: ^i32,
	next_idx: i32,
	idx: i32,
	into_table_var: i32,
	union_id: i32,
	sub_id: i16,
	query_total: i16,
}

/* All dynamic fields are initialized as needed.
 * Also note that queries only exist as pointers.
 */
new_query :: proc(sub_id: i16, query_text: string = "") -> ^Query {
	q := new(Query)
	q^ = {
		preview_text = query_text,
		top_count = max(type_of(q.top_count)),
		into_table_var = -1,
		sub_id = sub_id,
	}
	return q
}

@(private = "file")
_preflight :: proc(sql: ^Streamql, q: ^Query) -> Result {
	for subq in q.subquery_exprs {
		_preflight(sql, subq) or_return
	}
	for unionq in q.unions {
		_preflight(sql, unionq) or_return
	}

	for var_idx in q.var_expr_vars {
		return not_implemented()
	}

	for var_idx in q.var_source_vars {
		return not_implemented()
	}

	has_executed := q.plan.op_true == nil || .Is_Complete in q.plan.state
	op_reset(sql, q, has_executed) or_return

	for src in &q.sources {
		source_reset(&src, has_executed)
	}

	group_reset(q.groupby) or_return
	return group_reset(q.distinct_)
}

query_prepare :: proc(sql: ^Streamql, q: ^Query) -> Result {
	_preflight(sql, q) or_return
	op_preop(sql, q) or_return
	return plan_reset(&q.plan)
}

query_step :: proc(sql: ^Streamql, q: ^Query) -> Result {
	rows: int
	res := Result.Running

	for res == .Running && rows == 0 {
		rows, res = _exec_one_pass(q.plan.execute_vector)
	}

	q.plan.rows_affected = u64(rows)

	if res == .Complete {
		q.plan.state += {.Is_Complete}
	}

	return res
}

query_exec :: proc(sql: ^Streamql, q: ^Query) -> Result {
	res := Result.Running
	rows: int

	for res == .Running {
		rows, res = _exec_one_pass(q.plan.execute_vector)
		q.plan.rows_affected = u64(rows)
	}

	q.plan.state += {.Is_Complete}

	return res
}

query_exec_thread :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return not_implemented()
}

@(private = "file")
_exec_one_pass :: proc(exec_vector: []Process) -> (rows_affected: int, res: Result) {
	exec_vector := exec_vector
	res = .Complete

	for process in &exec_vector {
		if .Is_Enabled not_in process.state {
			continue
		}

		if _check_wait_list(process.wait_list) {
			res = .Running
			continue
		}

		org_rows := process.rows_affected

		#partial switch process.action__(&process) {
		case .Complete:
			process_disable(&process)
		case .Error:
			return 0, .Error
		case:
			res = .Running
		}
		
		if .Is_Op_True in process.state && org_rows != process.rows_affected {
			rows_affected = process.rows_affected
		}
	}

	return
}

query_add_source :: proc(sql: ^Streamql, q: ^Query, table_name, schema_name: string) -> Result {
	idx := len(q.sources)
	append_nothing(&q.sources)
	src := &q.sources[idx]
	construct_source(src, idx, table_name)

	if table_name[0] == '@' {
		append(&q.var_sources, i32(idx))
		src.props += {.Must_Reopen}
	}

	if util.string_compare_nocase("__stdin", table_name) == 0 {
		if ._Allow_Stdin in sql.config {
			src.props += {.Is_Stdin}
			sql.config -= {._Allow_Stdin}
			return .Ok
		}
	}

	if schema_name != "" {
		src.schema.name = strings.clone(schema_name)
		if schema_name[0] == '@' {
			src.schema.props += {.Is_Var}
		}
	}

	return .Ok
}

query_add_subquery_source :: proc(q: ^Query, subquery: ^Query) -> Result {
	idx := len(q.sources)
	append_nothing(&q.sources)
	src := &q.sources[len(q.sources) - 1]
	construct_source(src, idx, subquery)
	return .Ok
}

query_distribute_expression :: proc(q: ^Query, expr: ^Expression) -> (^Expression, Result) {
	if len(q.state.f_stack) > 0 {
		fn_expr := q.state.f_stack[len(q.state.f_stack) - 1]
		return function_add_expression(&fn_expr.data.(Expr_Function), expr), .Ok
	}
	switch q.state.mode {
	case .Select_List:
		return select_add_expression(&q.operation.(Select), expr), .Ok
	case .Update_List:
		return update_add_expression(&q.operation.(Update), expr)
	case .Top:
		q.top_expr = new(Expression)
		q.top_expr^ = expr^
		return q.top_expr, .Ok
	case .Case:
		return nil, .Error
	case .In:
		fallthrough
	case .Logic:
		return _add_logic_expression(q, expr)
	case .Groupby:
		return group_add_expression(q.groupby, expr), .Ok
	case .Orderby:
		return order_add_expression(q.orderby, expr), .Ok
	case .Set:
		fallthrough
	case .Declare:
		return set_set_init_expression(&q.operation.(Set), expr), .Ok
	case .Aggregate:
		fallthrough
	case .If:
		fallthrough
	case .Undefined:
		return nil, .Error
	}

	return nil, .Error
}

query_new_logic_item :: proc(q: ^Query, type: Logic_Group_Type, op_str: string = "") -> ^Logic_Group {
	lg : ^Logic_Group
	l_stack := &q.state.l_stack

	parent := l_stack[len(l_stack) - 1]
	if parent.type == nil {
		init_logic_group(parent, type, op_str)
		return parent
	} else if parent.items[0] == nil {
		parent.items[0] = new_logic_group(type, op_str)
		return parent.items[0]
	} else {
		parent.items[1] = new_logic_group(type, op_str)
		return parent.items[1]
	}

	unreachable()
}

@(private = "file")
_add_logic_expression :: proc(q: ^Query, expr: ^Expression) -> (^Expression, Result) {
	if _, ok := expr.data.(Expr_Aggregate); ok && q.state.l_stack[0] != q.having {
		fmt.fprintln(os.stderr, "cannot have aggregate logic outside of HAVING")
		return nil, .Error
	}

	lg := q.state.l_stack[len(q.state.l_stack) - 1]
	/* Allocate in init_logic_group if predicate */
	//if lg.condition == nil {
	//	lg.condition = new_logic()
	//}
	return logic_add_expression(lg.condition, expr), .Ok
}

@(private = "file")
_check_wait_list :: proc(wait_list: []^Process) -> bool {
	if len(wait_list) == 0 {
		return false
	}
	not_implemented()
	return false
}
