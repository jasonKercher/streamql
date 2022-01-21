package streamql

import "core:strings"
import "core:fmt"
import "core:os"
import "bigraph"

Plan :: struct {
	execute_vector: []Process,
	proc_graph: bigraph.Graph(Process),
	op_true: ^bigraph.Node(Process),
	op_false: ^bigraph.Node(Process),
	curr: ^bigraph.Node(Process),
	plan_str: string,
	src_count: u8,
	id: u8,
}
