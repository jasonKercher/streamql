package bigraph

import "core:container/queue"

Node :: struct($T: typeid) {
	data: T,
	out: [2]^Node(T),
	visit_count: i32,
	is_root: bool,
}
Graph :: struct($T: typeid) {
	nodes: [dynamic]^Node(T),
	roots: [dynamic]^Node(T),
	_trav: queue.Queue(^Node(T)),
	_root_idx: int,
}
