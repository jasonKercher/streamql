package streamql

Operation :: union {
	Select,
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
	into_table_name: string,
	top_expr: ^Expression,
	top_count: u64,
	idx: u32,
	next_idx: i32,
	union_id: i32,
	query_id: i16,
	query_total: i16,
}
