package streamql

import "core:fmt"

Sql_Parser :: struct {
	q:        string,
	lf_vec:   [dynamic]u32,
	tok_vec:  [dynamic]Token,
	tok_map:  map[string]Token_Type,
	curr_tok: ^Token,
}

sql_parser_init :: proc(self: ^Sql_Parser) {
	self^ = {
		lf_vec = make([dynamic]u32),
		tok_vec = make([dynamic]Token),
	}

}

sql_parser_parse :: proc(self: ^Sql_Parser, query_str: string) {
	self.q = query_str
	lex_lex(self)
}




