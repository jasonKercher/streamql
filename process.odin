package streamql

import "core:strings"

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
