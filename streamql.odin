package streamql

Config :: enum {
	Parse_Only,
}

Streamql :: struct {
	parser: Sql_Parser,
	config: bit_set[Config],
}

construct :: proc(self: ^Streamql, cfg: bit_set[Config] = {}) {
	self^ = {
		parser = make_parser(),
		config = cfg,
	}
}

destroy :: proc(self: ^Streamql) {
	parse_destroy(&self.parser)
}
