package streamql

Delete :: struct {
	writer: Writer,
	schema: Schema,
	top_count: i64,
}

delete_apply_process :: proc(q: ^Query) -> Result {
	return not_implemented()
}
