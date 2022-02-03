//+private
package streamql

import "fastrecs"

Record_Data :: union {
	fastrecs.Record,
}

Record :: struct {
	data: Record_Data,
	fields: []string,
	offset: i64,
	idx: i64,
	next: ^Record,
	root_fifo_idx: i32,
}

destroy_record :: proc(rec: ^Record) {

}
