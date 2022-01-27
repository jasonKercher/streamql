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
}

destroy_record :: proc(rec: ^Record) {

}
