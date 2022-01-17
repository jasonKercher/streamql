package streamql

Writer :: struct {
	type: Io,
}

make_writer :: proc(sql: ^Streamql, write_io: Io) -> Writer {
	return Writer { type = write_io }
}
