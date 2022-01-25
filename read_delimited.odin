package streamql

import "fastrecs"

delimited_delete :: proc(r: ^Reader) {
	fastrecs.destroy(&r.data.(fastrecs.Reader))
}

delimited_reset :: proc(r: ^Reader) -> Result {
	return not_implemented()
}

delimited_reset_stdin :: proc(r: ^Reader) -> Result {
	return not_implemented()
}

delimited_get_record :: proc(r: ^Reader, rec: ^Record) -> Result {
	return not_implemented()
}

delimited_get_record_at :: proc(r: ^Reader, rec: ^Record, offest: i64) -> Result {
	return not_implemented()
}
