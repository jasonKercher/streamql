package streamql

import "core:strings"
import "core:fmt"
import "core:os"
import "bigraph"

Plan_State :: enum {
	Has_Stepped,
	Is_Complete,
	Is_Const,
}

Plan :: struct {
	execute_vector: []Process,
	proc_graph: bigraph.Graph(Process),
	op_true: ^bigraph.Node(Process),
	op_false: ^bigraph.Node(Process),
	curr: ^bigraph.Node(Process),
	plan_str: string,
	state: bit_set[Plan_State],
	src_count: u8,
	id: u8,
}
