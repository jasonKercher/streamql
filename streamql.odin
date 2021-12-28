package streamql

Config :: enum {
	Parse_Only,
}

Streamql :: struct {
	parser: Sql_Parser,
	queries: [dynamic]Query,
	config: bit_set[Config],
}

construct :: proc(self: ^Streamql, cfg: bit_set[Config] = {}) {
	self^ = {
		parser = make_parser(),
		queries = make([dynamic]Query),
		config = cfg,
	}
}

destroy :: proc(self: ^Streamql) {
	parse_destroy(&self.parser)
}

make_plans :: proc(self: ^Streamql, query_str: string) -> Sql_Result {
	parse_parse(self, query_str) or_return


	return .Ok
}

exec :: proc(self: ^Streamql, query_str: string) -> Sql_Result {
	make_plans(self, query_str) or_return

	
	return .Ok
}

reset :: proc(self: ^Streamql) {
}
