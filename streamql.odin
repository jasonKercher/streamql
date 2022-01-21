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
