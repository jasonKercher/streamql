package streamql

import "core:math/bits"
import "core:strings"
import "core:fmt"
import "core:os"

PIPE_MIN :: 2
PIPE_MAX :: 1024
PIPE_DEFAULT :: 16
PIPE_DEFAULT_THREAD :: 64

Config :: enum u8 {
	Check,
	Strict,
	Thread,
	Overwrite,
	Summarize,
	Parse_Only,
	Print_Plan,
	Force_Cartesian,
	Add_Header,
	No_Header, /* lol? */
	_Allow_Stdin,
	_Delim_Set,
	_Rec_Term_Set,
	_Schema_Paths_Resolved,
}

Verbose ::enum u8 {
	Quiet,
	Basic,
	Noisy,
	Debug,
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
	Running,
	Complete,
	Eof,
	Null, // refering to NULL in SQL
	_Waiting_In0,
	_Waiting_In1,
	_Waiting_In_Either,
	_Waiting_In_Both,
	_Waiting_Out0,
	_Waiting_Out1,
}

/* maybe just do: i128, f128 ?? */
Data :: union {
	i64,
	f64,
	string,
}

Data_Type :: enum {
	Int,
	Float,
	String,
}

Api :: struct {
	fields: []Field,
	_sb: strings.Builder,
}

Field :: struct {
	name: string,
	data: Data,
	is_null: bool,
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
	api: ^Api,
	default_schema: string,
	schema_map: map[string]^Schema,
	schema_paths: [dynamic]string,
	queries: [dynamic]^Query,
	variables: [dynamic]Variable,
	scopes: [dynamic]Scope,
	in_delim: string,
	out_delim: string,
	rec_term: string,
	query_idx: i32,
	curr_scope: i32,
	pipe_factor: u32,
	config: bit_set[Config],
	branch_state: _Branch_State,
	in_quotes: Quotes,
	out_quotes: Quotes,
	verbosity: Verbose,
}

construct :: proc(sql: ^Streamql, cfg: bit_set[Config] = {}) {
	sql^ = {
		parser = make_parser(),
		schema_paths = make([dynamic]string),
		queries = make([dynamic]^Query),
		scopes = make([dynamic]Scope),
		config = cfg,
	}

	sql.pipe_factor = .Thread in cfg ? PIPE_DEFAULT_THREAD : PIPE_DEFAULT

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

exec_plans :: proc(sql: ^Streamql, limit: int = bits.I32_MAX) -> Result {
	res: Result

	i := int(sql.query_idx)

	for ; i < len(sql.queries); i += 1 {
		if .Has_Stepped in sql.queries[i].plan.state {
			fmt.eprintln("Cannot execute plan that has stepped")
			return .Error
		}

		if sql.verbosity > Verbose.Basic {
			fmt.printf("EXEC: %s\n", sql.queries[i].preview_text)
		}

		if res = query_prepare(sql, sql.queries[i]); res == .Error {
			break
		}

		if .Thread in sql.config {
			res = query_exec_thread(sql, sql.queries[i])
		} else {
			res = query_exec(sql, sql.queries[i])
		}

		if res == .Error {
			break
		}

		if sql.verbosity > Verbose.Quiet {
			_print_footer(sql.queries[i])
		}
		i = int(sql.queries[i].next_idx)
	}

	sql.query_idx = i32(i)

	/* Add option to keep queries? */
	if res == .Error || i >= len(sql.queries) {
		reset(sql)
	}

	return res
}

exec :: proc(sql: ^Streamql, query_str: string) -> Result {
	generate_plans(sql, query_str) or_return
	if .Check in sql.config {
		reset(sql)
		return .Ok
	}

	return exec_plans(sql)
}

step :: proc(sql: ^Streamql) -> (fields: []Field, res: Result) {
	q := sql.queries[sql.query_idx]
	if .Has_Stepped not_in q.plan.state {
		if sql.api == nil {
			_api_connect(sql)
		}
		q.plan.state += {.Has_Stepped}

		query_prepare(sql, q) or_return

		for p in &q.plan.execute_vector {
			p.max_iters = 1
		}
	}

	res = query_step(sql, q)
	if res == .Running {
		return sql.api.fields, .Running
	}

	delete(sql.api.fields)
	free(sql.api)
	sql.api = nil

	q.plan.state -= {.Has_Stepped}

	if res == .Error || int(sql.query_idx) >= len(sql.queries) {
		reset(sql)
	}

	return
}

reset :: proc(sql: ^Streamql) {
	clear(&sql.queries)
	clear(&sql.scopes)

	sql.query_idx = 0

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

@private
not_implemented :: proc(loc := #caller_location) -> Result {
	fmt.fprintln(os.stderr, "not implemented:", loc)
	return .Error
}

@private
_print_footer :: proc(q: ^Query) {
	not_implemented()
}

@private
_api_connect :: proc(sql: ^Streamql) -> Result {
	q := sql.queries[sql.query_idx]
	sel, is_sel := &q.operation.(Select)
	if !is_sel || q.into_table_name != "" {
		fmt.eprintln("can only step through SELECT queries")
		return .Error
	}

	sql.api = new(Api)
	select_connect_api(q, sql.api) or_return

	if q.orderby != nil {
		order_connect_api(q, sql.api)
	}

	/* If we are using the API, we must read every field */
	for p in &q.plan.execute_vector {
		if p.action__ == sql_read {
			src := p.data.(^Source)
			r := &src.schema.data.(Reader)
			r.max_field_idx = max(i32)
		}
	}

	return .Ok
}


