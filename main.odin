package streamql
import "core:math/bits"

import "core:strings"
import "core:os"
import "getargs"
import "core:fmt"
import "bigraph"


main :: proc()
{
	query_str : string

	a := getargs.make_getargs()
	getargs.read_args(&a, os.args)

	cfg: bit_set[Config]

	sql: Streamql

		query_str = "select 1"

	if exec(&sql, query_str) == .Error {
		os.exit(2)
	}

}



op_writer_init :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return .Ok
}

op_apply_process :: proc(q: ^Query, is_subquery: bool) -> Result {
	return .Ok
}

op_set_writer :: proc(gen: ^Operation, w: ^Writer) {
	gen := gen
	#partial switch op in gen {
	case Select:
		op.writer = w^
	}
}


Plan :: struct {
	execute_vector: []Process,
	proc_graph: bigraph.Graph(Process),
	op_true: ^bigraph.Node(Process),
	op_false: ^bigraph.Node(Process),
	curr: ^bigraph.Node(Process),
	plan_str: string,
	src_count: u8,
	id: u8,
}


Process_Data :: union {
	^Source,
	^Select,
}

Process :: struct {
	data: Process_Data,
	msg: string,
	plan_id: u8,
	in_src_count: u8,
	out_src_count: u8,
}


Operation :: union {
	Select,
}

Query :: struct {
	plan: Plan,
}


Select_Call :: proc(sel: ^Select) -> Result

Select :: struct {
	select__: Select_Call,
	writer: Writer,
	top_count: i64,
}

make_select :: proc() -> Select {
	return Select {
		top_count = 0,
	}
}


Source_Data :: union {
	^Query,
	string,
}

Source :: struct {
	data: Source_Data,
	alias: string,
}

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

exec :: proc(sql: ^Streamql, query_str: string) -> Result {
	return .Ok
}


import "core:c"

foreign import libc "system:c"
foreign libc {
	@(link_name="mkstemp") _libc_mkstemp :: proc(template: cstring) -> c.int ---
}

Writer_Data :: union {
	Delimited_Writer,
	Fixed_Writer,
	Subquery_Writer,
}

Writer :: struct {
	data: Writer_Data,
	file_name: string,
	temp_name: string,
	//temp_node: ^linkedlist.Node(string),
	fd: os.Handle,
	is_detached: bool,
}

Delimited_Writer :: struct {

}

Fixed_Writer :: struct {

}

Subquery_Writer :: struct {

}

