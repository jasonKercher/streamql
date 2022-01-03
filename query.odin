package streamql

import "core:os"
import "core:fmt"
import "core:strings"
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
	joinable_logic: [dynamic]^Logic,
	into_table_name: string,
	preview_text: string,
	state: ^Listener_State,
	top_count: u64,
	top_expr: ^Expression,
	idx: u32,
	next_idx: u32,
	into_table_var: i32,
	union_id: i32,
	sub_id: i16,
	query_total: i16,
}

/* All dynamic fields are initialized as needed.
 * Also note that queries only exist as pointers.
 */
new_query :: proc(sub_id: i16) -> ^Query {
	q := new(Query)
	q^ = {
		top_count = bits.U64_MAX,
		into_table_var = -1,
		sub_id = sub_id,
	}
	return q
}

query_add_source :: proc(sql: ^Streamql, q: ^Query, table_name, schema_name: string) -> Result {
	idx := i32(len(q.sources))
	append_nothing(&q.sources)
	src := &q.sources[idx]
	source_construct(src, table_name)

	if table_name[0] == '@' {
		append(&q.var_sources, idx)
		src.props += {.Must_Reopen}
	}

	if table_name == "__STDIN" {
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
	append_nothing(&q.sources)
	src := &q.sources[len(q.sources) - 1]
	source_construct(src, subquery)
	return .Ok
}

query_distribute_expression :: proc(q: ^Query, expr: ^Expression) -> (^Expression, Result) {
	if len(q.state.f_stack) > 0 {
		fn_expr := q.state.f_stack[len(q.state.f_stack) - 1]
		return function_add_expression(&fn_expr.data.(Function), expr), .Ok
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

query_new_logic_item :: proc(q: ^Query, type: Logic_Group_Type) -> ^Logic_Group {
	lg : ^Logic_Group
	l_stack := &q.state.l_stack

	parent := l_stack[len(l_stack) - 1]
	if parent.type == .Unset {
		lg = parent
		lg.type = type
	} else if parent.items[0] == nil {
		lg = new_logic_group(type)
		parent.items[0] = lg
	} else {
		lg = new_logic_group(type)
		parent.items[1] = lg
	}

	return lg
}

@(private = "file")
_add_logic_expression :: proc(q: ^Query, expr: ^Expression) -> (^Expression, Result) {
	if _, ok := expr.data.(Aggregate); ok && q.state.l_stack[0] != q.having {
		fmt.fprintln(os.stderr, "cannot have aggregate logic outside of HAVING")
		return nil, .Error
	}

	lg := q.state.l_stack[len(q.state.l_stack) - 1]
	if lg.condition == nil {
		lg.condition = new_logic()
	}
	return logic_add_expression(lg.condition, expr), .Ok
}


