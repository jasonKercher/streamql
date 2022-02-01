//+private
package streamql

import "core:strings"

Process_State :: enum u8 {
	Is_Const,
	Is_Enabled,
	Is_Passive,
	Is_Op_True,
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

Process_Result :: enum u8 {
	Ok,
	Error,
	Complete,
	Running,
	Wait_On_In0,
	Wait_On_In1,
	Wait_On_In_Either,
	Wait_On_In_Both,
	Wait_On_Out0,
	Wait_On_Out1,
}

Process_Call :: proc(process: ^Process) -> Process_Result

Process :: struct {
	data: Process_Data,
	action__: Process_Call,
	wait_list: []^Process,
	msg: string,
	rows_affected: int,
	state: bit_set[Process_State],
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

process_destroy :: proc(process: ^Process) {
	delete(process.msg)
}

process_enable :: proc(process: ^Process) {
	not_implemented()
}
process_disable :: proc(process: ^Process) {
	not_implemented()
}
process_add_to_wait_list :: proc(waiter: ^Process, waitee: ^Process) {
	not_implemented()
}
