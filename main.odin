package streamql
import "core:math/bits"
import "core:strings"
import "core:os"
import "getargs"
import "core:fmt"

main :: proc()
{
	a := getargs.make_getargs()
	getargs.read_args(&a, os.args)
	sql: Streamql
	query_str := "select 1"
	exec(&sql, query_str)
}

exec :: proc(sql: ^Streamql, query_str: string) -> Result {
	return .Ok
}

shnt :: proc() -> Result {
	return .Ok
}

Node :: struct($T: typeid) {
	data: T,
}

Plan :: struct {
	curr: ^Node(Process),
}

Process_Data :: union {
	^Source,
	^Select,
}

Process :: struct {
	data: Process_Data,
}

Operation :: union {
	Select,
}

Query :: struct {
	plan: Plan,
}

Select :: struct {
	writer: Writer,
}

Source_Data :: union {
	^Query,
}

Source :: struct {
	data: Source_Data,
}

Result :: enum {
	Ok,
	Error,
}

Streamql :: struct {
	queries: [dynamic]^Query,
}

Writer_Data :: union {
	Subquery_Writer,
}

Writer :: struct {
	data: Writer_Data,
}

Subquery_Writer :: struct {}
