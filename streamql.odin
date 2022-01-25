package streamql

import "core:strings"
import "core:fmt"
import "core:os"

Config :: enum u8 {
	Strict,
	Overwrite,
	Summarize,
	Parse_Only,
	Print_Plan,
	Force_Cartesian,
	_Allow_Stdin,
	_Delim_Set,
	_Rec_Term_Set,
	_Schema_Paths_Resolved,
}

Quotes :: enum u8 {
	None,
	Weak,
	Rfc4180,
	All,
}

Result :: enum u8 {
	Ok,
	Error,
}

@private
_Branch_State :: enum u8 {
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
	in_delim: string,
	out_delim: string,
	rec_term: string,
	curr_scope: i32,
	config: bit_set[Config],
	branch_state: _Branch_State,
	in_quotes: Quotes,
	out_quotes: Quotes,
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
	if plan_build(sql) == .Error {
		reset(sql)
		return .Error
	}
	if .Print_Plan in sql.config {
		plan_print(sql)
	}
	return .Ok
}

exec :: proc(sql: ^Streamql, query_str: string) -> Result {
	generate_plans(sql, query_str) or_return
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
			fmt.eprintf("`%s' does not appear to be a directory\n", path)
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
