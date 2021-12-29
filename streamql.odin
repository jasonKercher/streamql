package streamql

Config :: enum {
	Parse_Only,
}

Result :: enum {
	Ok,
	Error,
}

@(private)
_Branch_State :: enum {
	No_Branch,
	Expect_Expr,
	Expect_Else,
	Expect_Exit,
}

Streamql :: struct {
	parser: Parser,
	listener: Listener,
	queries: [dynamic]^Query,
	variables: [dynamic]Variable,
	scopes: [dynamic]Scope,
	curr_scope: i32,
	config: bit_set[Config],
	branch_state: _Branch_State,
}

construct :: proc(sql: ^Streamql, cfg: bit_set[Config] = {}) {
	sql^ = {
		parser = make_parser(),
		queries = make([dynamic]^Query),
		scopes = make([dynamic]Scope),
		config = cfg,
	}

	/* scopes[0] == global scope */
	append(&sql.scopes, make_scope())
}

destroy :: proc(sql: ^Streamql) {
	parse_destroy(&sql.parser)
}

generate_plans :: proc(sql: ^Streamql, query_str: string) -> Result {
	parse_parse(sql, query_str) or_return


	return .Ok
}

exec :: proc(sql: ^Streamql, query_str: string) -> Result {
	generate_plans(sql, query_str) or_return

	
	return .Ok
}

reset :: proc(sql: ^Streamql) {
}
