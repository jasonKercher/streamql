package streamql

import "core:os"
import "core:fmt"
import "core:strings"
import "core:math/bits"

Operation :: union {
	//Set,
	//Branch,
	Select,
	//Update,
	//Delete,
}

Query :: struct {
	operation: Operation,
	plan: Plan,
	sources: [dynamic]Source,
	//groupby: ^Group,
	//distinct_: ^Group,
	//orderby: ^Order,
	unions: [dynamic]^Query,
	subquery_exprs: [dynamic]^Query,
	var_source_vars: [dynamic]i32,
	var_sources: [dynamic]i32,
	var_expr_vars: [dynamic]i32,
	var_exprs: [dynamic]i32,
	into_table_name: string,
	preview_text: string,
	state: ^Listener_State,
	top_count: i64,
	top_expr: ^Expression,
	next_idx_ref: ^u32,
	next_idx: u32,
	idx: u32,
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
		top_count = bits.I64_MAX,
		into_table_var = -1,
		sub_id = sub_id,
	}
	return q
}
