//+private
package streamql

import "core:strings"
import "core:fmt"
import "bigraph"
import "fifo"

Process_State :: enum u8 {
	Is_Const,
	Is_Dual_Link,
	Is_Enabled,
	Is_Passive,
	Is_Op_True,
	Is_Debug,
	In0_Always_Dead,
	Kill_In0,
	Kill_In1,
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

Process_Unions :: struct #raw_union {
	n: []^bigraph.Node(Process),
	p: []^Process,
}

Process :: struct {
	data: Process_Data,
	action__: Process_Call,
	root_fifo_ref: ^[]fifo.Fifo(^Record),
	wait_list: []^Process,
	waitee: ^Process,
	input: [2]^fifo.Fifo(^Record),
	output: [2]^fifo.Fifo(^Record),
	aux_root: ^fifo.Fifo(^Record),
	union_data: Process_Unions,
	msg: string,
	rows_affected: int,
	_in_buf: []^Record,
	_in_buf_idx: u32,
	max_iters: u16,
	state: bit_set[Process_State],
	plan_id: u8,
	in_src_count: u8,
	out_src_count: u8,
}

make_process :: proc(plan: ^Plan, msg: string) -> Process {
	if plan == nil {
		return Process {
			msg = strings.clone(msg),
			state = {.Is_Enabled, .Wait_In0},
			max_iters = max(u16),
		}
	}
	return Process {
		msg = strings.clone(msg),
		plan_id = plan.id,
		in_src_count = plan.src_count,
		out_src_count = plan.src_count,
		state = {.Is_Enabled, .Wait_In0},
		max_iters = max(u16),
	}
}

process_destroy :: proc(process: ^Process) {
	delete(process.msg)
}

process_activate :: proc(process: ^Process, root_fifo_vec: ^[dynamic]fifo.Fifo(^Record), pipe_count: ^int, base_size: int) {
	process._in_buf = make([]^Record, u16(base_size))
	if .Root_Fifo0 in process.state {
		process.input[0] = _new_fifo_root(root_fifo_vec, pipe_count)
	} else if .Root_Fifo1 in process.state {
		process.input[1] = _new_fifo_root(root_fifo_vec, pipe_count)
	}

	if select, is_select := process.data.(^Select); is_select {
		if .Is_Const in process.state {
			select.schema.props += {.Is_Const}
		}
	}

	for node in process.union_data.n {
		pipe_count^ += 1
		node.data.output[0] = fifo.new_fifo(^Record, u16(base_size))
		node.data.output[0].input_count = 1
	}

	if process.input[0] == nil {
		pipe_count^ += 1
		process.input[0] = fifo.new_fifo(^Record, u16(base_size))
		/* NOTE: GROUP BY hack. a constant query expression
		 *       containing a group by essentially has 2 roots.
		 *       We just give in[0] a nudge (like a root).
		 */
		if process.action__ == sql_groupby && .Is_Const in process.state {
			fifo.advance(process.input[0])
		}
	}

	if .Has_Second_Input in process.state {
		pipe_count^ += 1
		process.input[1] = fifo.new_fifo(^Record, u16(base_size))
	}

	if .Kill_In0 in process.state {
		process.input[0].is_open = false
	}

	if .Kill_In1 in process.state {
		process.input[1].is_open = false
	}
	
	if .Needs_Aux in process.state {
		pipe_count^ += 1
		process.aux_root = fifo.new_fifo(^Record, u16(base_size))
	}
}

process_enable :: proc(process: ^Process) {
	if process._in_buf != nil {
		sqlprocess_recycle(process, process._in_buf, &process._in_buf_idx)
	}

	/* TODO: queued_results?? */

	process.rows_affected = 0
	fifo.reset(process.input[0])
	fifo.reset(process.input[1])
	fifo.reset(process.output[0])
	fifo.reset(process.output[1])

	if .Kill_In0 in process.state {
		fifo.set_open(process.input[0], false)
	}
	if .Kill_In1 in process.state {
		fifo.set_open(process.input[1], false)
	}

	if .In0_Always_Dead not_in process.state {
		process.state += {.Wait_In0}
	}
	process.state += {.Is_Enabled}

	/* GROUP BY hack from process_activate */
	if process.action__ == sql_groupby && .Is_Const in process.state {
		fifo.advance(process.input[0])
	}
}

@(private = "file")
_recycle_all :: proc(process: ^Process, f: ^fifo.Fifo(^Record)) {
	for rec := fifo.begin(f); rec != fifo.end(f); rec = fifo.iter(f) {
		sqlprocess_recycle(process, rec)
	}
	fifo.update(f)
}

process_disable :: proc(process: ^Process) {
	if .Is_Enabled not_in process.state {
		return
	}

	if process.output[0] != nil {
		fifo.set_open(process.output[0], false)
	}
	if process.output[1] != nil {
		fifo.set_open(process.output[1], false)
	}
	process.state -= {.Is_Enabled}
	fifo.set_open(process.input[0], false)

	/* Recycle any remaining records */
	if .Root_Fifo0 not_in process.state {
		_recycle_all(process, process.input[0])
	}
	if process.input[1] != nil && .Root_Fifo1 not_in process.state {
		_recycle_all(process, process.input[1])
	}

	/* If this process is causing another process to wait,
	 * then it is time to let it know we are done.
	 */
	if process.waitee != nil {
		not_implemented()
	}

	if .Is_Debug in process.state {
		print_len := len(process.msg) > 25 ? 25 : len(process.msg)
		fmt.eprintf("Process `%.*s' stopped\n", print_len, process.msg)
	}
}

process_add_to_wait_list :: proc(waiter: ^Process, waitee: ^Process) {
	not_implemented()
}

@(private = "file")
_new_fifo_root :: proc(root_fifo_vec: ^[dynamic]fifo.Fifo(^Record), pipe_count: ^int) -> ^fifo.Fifo(^Record) {
	append(root_fifo_vec, fifo.make_fifo(^Record))
	pipe_count^ += 1
	return &root_fifo_vec[len(root_fifo_vec) - 1]
}

