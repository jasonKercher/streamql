package streamql

import "core:strings"
import "core:fmt"
import "core:os"

Config :: enum {
	Strict,
	Overwrite,
	Summarize,
	Parse_Only,
	Force_Cartesian,
	_Allow_Stdin,
	_Delim_Set,
	_Rec_Term_Set,
	_Schema_Paths_Resolved,
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
	default_schema: string,
	schema_map: map[string]^Schema,
	schema_paths: [dynamic]string,
	queries: [dynamic]^Query,
	variables: [dynamic]Variable,
	scopes: [dynamic]Scope,
	out_delim: string,
	rec_term: string,
	curr_scope: i32,
	config: bit_set[Config],
	branch_state: _Branch_State,
}

construct :: proc(sql: ^Streamql, cfg: bit_set[Config] = {}) {
	sql^ = {
		parser = make_parser(),
		schema_paths = make([dynamic]string),
		queries = make([dynamic]^Query),
		scopes = make([dynamic]Scope),
		config = cfg,
	}

	/* scopes[0] == global scope */
	append(&sql.scopes, make_scope())
}

destroy :: proc(sql: ^Streamql) {
	destroy_parser(&sql.parser)
}

generate_plans :: proc(sql: ^Streamql, query_str: string) -> Result {
	if parse_parse(sql, query_str) ==  .Error {
		reset(sql)
		return .Error
	}
	if schema_resolve(sql) == .Error {
		reset(sql)
		return .Error
	}

	return .Ok
}

exec :: proc(sql: ^Streamql, query_str: string) -> Result {
	generate_plans(sql, query_str) or_return


	if len(sql.queries) == 0 {
		return .Ok
	}
	q := sql.queries[len(sql.queries) - 1]
	s := &q.operation.(Select)

	b := strings.make_builder()
	first := true
	for expr in &s.expressions {
		if !first {
			strings.write_byte(&b, ',')
		}
		first = false
		expression_cat_description(&expr, &b)
	}

	fmt.println(strings.to_string(b))
	
	return .Ok
}

reset :: proc(sql: ^Streamql) {
	clear(&sql.queries)
	clear(&sql.scopes)

	/* scopes[0] == global scope */
	append(&sql.scopes, make_scope())
}

add_schema_path :: proc(sql: ^Streamql, path: string, throw: bool = true) -> Result {
	if !os.is_dir(path) {
		if throw {
			fmt.fprintf(os.stderr, "`%s' does not appear to be a directory\n", path)
		}
		return .Error
	}

	append(&sql.schema_paths, strings.clone(path))
	return .Ok
}

not_implemented :: proc(loc := #caller_location) -> Result {
	fmt.fprintln(os.stderr, "not implemented:", loc)
	return .Error
}
