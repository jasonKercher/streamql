//+private
package streamql

import "core:c"
import "core:os"
import "core:fmt"
import "linkedlist"
import "core:strings"
import "core:path/filepath"

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
	temp_node: ^linkedlist.Node(string),
	fd: os.Handle,
	type: Io,
	is_detached: bool,
}

make_writer :: proc(sql: ^Streamql, write_io: Io) -> Writer {
	new_writer := Writer {
		type = write_io,
		fd = -1,
	}
	switch write_io {
	case .Delimited:
		new_writer.data = make_delimited_writer()
	case .Fixed:
		new_writer.data = make_fixed_writer()
	case .Subquery:
		new_writer.data = make_subquery_writer()
	}

	return new_writer
}

destroy_writer :: proc(w: ^Writer) {
	not_implemented()
}

writer_open :: proc(w: ^Writer, file_name: string) -> Result {
	_set_file_name(w, file_name)
	if _is_open(w) {
		fmt.eprintf("writer already open")
		return .Error
	}
	return _make_temp_file(w)
}

writer_close :: proc(w: ^Writer) -> Result {
	if !_is_open(w) {
		return .Ok
	}

	os.close(w.fd)
	w.fd = -1

	if w.is_detached {
		w.is_detached = false
		return .Ok
	}

	if os.rename(w.temp_name, w.file_name) != os.ERROR_NONE {
		fmt.eprintf("rename failed\n")
		return .Error
	}

	/* chmod ?? */

	linkedlist.remove(&_global_remove_list, w.temp_node)
	w.temp_node = nil
	return .Ok
}

writer_resize :: proc(w: ^Writer, n: int) {
	//not_implemented()
}

writer_set_delim :: proc(w: ^Writer, delim: string) {
	not_implemented()
}

writer_set_rec_term :: proc(w: ^Writer, delim: string) {
	not_implemented()
}

writer_take_file_name :: proc(w: ^Writer) -> string {
	w.is_detached = true
	file_name := w.file_name
	w.file_name = ""
	return file_name
}

writer_export_temp :: proc(w: ^Writer) -> (file_name: string, res: Result) {
	if !_is_open(w) {
		_make_temp_file(w) or_return
	}
	if w.is_detached {
		linkedlist.remove(&_global_remove_list, w.temp_node)
		return w.temp_name, .Ok
	} else {
		return w.file_name, .Ok
	}
}

@(private = "file")
_make_temp_file :: proc(w: ^Writer) -> Result {
	dir_name := "."
	if w.file_name != "" {
		dir_name = filepath.dir(w.file_name)
	}
	temp_name := fmt.tprintf("%s/_write_XXXXXX", dir_name)
	temp_name_cstr := strings.clone_to_cstring(temp_name, context.temp_allocator)

	fd := _libc_mkstemp(temp_name_cstr)
	if fd == -1 {
		return .Error
	}

	w.temp_node = linkedlist.push(&_global_remove_list, temp_name)

	return .Ok
}

@(private = "file")
_set_file_name :: proc(w: ^Writer, file_name: string) {
	w.file_name = file_name
	w.is_detached = false
}

@(private = "file")
_is_open :: proc(w: ^Writer) -> bool {
	return w.fd != -1 && w.fd != os.stdout
}

/** Delimited_Writer **/

Delimited_Writer :: struct {

}

make_delimited_writer :: proc() -> Delimited_Writer {
	return Delimited_Writer {}
}

/** Fixed_Writer **/

Fixed_Writer :: struct {

}

make_fixed_writer :: proc() -> Fixed_Writer {
	return Fixed_Writer {}
}

/** Subquery_Writer **/

Subquery_Writer :: struct {

}

make_subquery_writer :: proc() -> Subquery_Writer {
	return Subquery_Writer {}
}

