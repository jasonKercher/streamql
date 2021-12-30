package streamql

import "core:fmt"

Config :: enum {
	Parse_Only,
	_Allow_Stdin,
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


	if len(sql.queries) == 0 {
		return .Ok
	}
	q := sql.queries[len(sql.queries) - 1]
	s := &q.operation.(Select)
	for expr in s.expressions {
		fmt.println(expr.data)
	}
	
	return .Ok
}

reset :: proc(sql: ^Streamql) {
}
