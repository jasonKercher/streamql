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

@private
_Branch_State :: enum {
	No_Branch,
	Expect_Expr,
	Expect_Else,
	Expect_Exit,
}

Streamql :: struct {
	default_schema: string,
	schema_paths: [dynamic]string,
	queries: [dynamic]^Query,
	out_delim: string,
	rec_term: string,
	config: bit_set[Config],
	branch_state: _Branch_State,
}

construct :: proc(sql: ^Streamql, cfg: bit_set[Config] = {}) {
	sql^ = {
		schema_paths = make([dynamic]string),
		queries = make([dynamic]^Query),
		config = cfg,
	}

}

destroy :: proc(sql: ^Streamql) {
	delete(sql.queries)
}

exec :: proc(sql: ^Streamql, query_str: string) -> Result {
	return .Ok
}

