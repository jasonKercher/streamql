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
	into_table_name: string,
	preview_text: string,
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

query_distribute_expression :: proc(sql: ^Streamql, q: ^Query, expr: ^Expression) -> Result {
	l := &sql.listener
	if len(l.state.f_stack) > 0 {
		fn_expr := &l.state.f_stack[len(l.state.f_stack) - 1]
		function_add_expression(&fn_expr.data.fn, expr)
		return .Ok
	}
	switch l.state.mode {
	case .Select_List:
		select_add_expression(&q.operation.(Select), expr)
	case .Update_List:
		return update_add_expression(&q.operation.(Update), expr)
	case .Top:
		q.top_expr = new(Expression)
		q.top_expr^ = expr^
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
		set_set_init_expression(&q.operation.(Set), expr)
	case .Aggregate:
		fallthrough
	case .If:
		return .Error
	}

	return .Ok
}

@(private = "file")
_add_logic_expression :: proc(sql: ^Streamql, q: ^Query, expr: ^Expression) -> Result {
	if sql.listener.state.l_mode != .Having && expr.type == .Aggregate {
		fmt.fprintln(os.stderr, "cannot have aggregate logic outside of HAVING")
		return .Error
	}

	lg := sql.listener.state.l_stack[len(sql.listener.state.l_stack) - 1]
	if lg.condition == nil {
		lg.condition = new_logic()
	}
	logic_add_expression(lg.condition, expr)

	return .Ok
}
