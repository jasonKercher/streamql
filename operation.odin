package streamql

op_get_schema :: proc(gen: ^Operation) -> ^Schema {
	switch op in gen {
	case Select:
		return &op.schema
	case Update:
		return &op.schema
	case Delete:
		return &op.schema
	case Branch:
		return nil
	case Set:
		return nil
	}

	return nil
}

op_set_delim :: proc(gen: ^Operation, delim: string) {
	schema_set_delim(op_get_schema(gen), delim)
}
op_set_rec_term :: proc(gen: ^Operation, rec_term: string) {
	schema_set_rec_term(op_get_schema(gen), rec_term)
}
