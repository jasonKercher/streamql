//+private
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
	^Logic_Group,
	^Select,
}

Process_Call :: proc(process: ^Process) -> Result

Process :: struct {
	data: Process_Data,
	action__: Process_Call,
	msg: string,
	props: bit_set[Process_Props],
	plan_id: u8,
	in_src_count: u8,
	out_src_count: u8,
}

make_process :: proc(plan: ^Plan, msg: string) -> Process {
	if plan == nil {
		return Process {
			msg = strings.clone(msg),
		}
	}
	return Process {
		msg = strings.clone(msg),
		plan_id = plan.id,
		in_src_count = plan.src_count,
		out_src_count = plan.src_count,
	}
}

process_add_to_wait_list :: proc(waiter: ^Process, waitee: ^Process) {
	not_implemented()
}
