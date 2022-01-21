package streamql

//import "util"
import "core:c"
import "core:os"
import "core:fmt"
//import "linkedlist"
import "core:strings"

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

