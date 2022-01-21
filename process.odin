package streamql

import "core:strings"

Process_Props :: enum {
	Is_Const,
	Is_Enabled,
	Is_Passive,
	Wait_In0,
	Wait_In0_End,
	Root_Fifo0,
	Root_Fifo1,
	Needs_Aux,
	Is_Secondary,
	Has_Second_Input,
}

Process_Data :: union {
	^Source,
	^Select,
}


Process :: struct {
	data: Process_Data,
	msg: string,
	props: bit_set[Process_Props],
	plan_id: u8,
	in_src_count: u8,
	out_src_count: u8,
}
