package streamql

//import "fastrecs"

Get_Record_Call :: proc(r: ^Reader, rec: ^Record) -> Result
Get_Record_At_Call :: proc(r: ^Reader, rec: ^Record, offset: i64) -> Result
Reset_Call :: proc(r: ^Reader) -> Result

Reader :: struct {
	reset__: Reset_Call,
	get_record__: Get_Record_Call,
	get_record_at__: Get_Record_At_Call,
	//data: fastrecs.Reader,
	first_rec: Record,
	skip_rows: i64,
	max_idx: i32,
}

reader_assign :: proc(sql: ^Streamql, src: ^Source) -> Result {
	return not_implemented()
}

reader_start_file_backed_input :: proc(r: ^Reader) {
	not_implemented()
}
