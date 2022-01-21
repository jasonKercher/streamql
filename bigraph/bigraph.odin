package bigraph

import "core:container/queue"

/* This is linked list style directed graph. Each
 * node has 2 outputs (hence the name). This isn't
 * meant to be fast. I wouldn't use this for 
 * anything big.  It is just easy to work with.
 */

Node :: struct($T: typeid) {
	data: T,
	out: [2]^Node(T),
	visit_count: i32,
	is_root: bool,
}

new_node :: proc(val: $T) -> ^Node(T) {
	new_node := new(Node(T))
	new_node^ = {
		data = val,
	}
	return new_node
}

free_node :: proc(node: ^Node($T)) -> T {
	val := node.data
	free(node)
	return val
}

Graph :: struct($T: typeid) {
	nodes: [dynamic]^Node(T),
	roots: [dynamic]^Node(T),
	_trav: queue.Queue(^Node(T)),
	_root_idx: int,
}

make_graph :: proc($T: typeid) -> Graph(T) {
	new_graph := Graph(T) {
		nodes = make([dynamic]^Node(T)),
		roots = make([dynamic]^Node(T)),
		_root_idx = -1,
	}

	queue.init(&new_graph._trav)
	return new_graph
}

destroy :: proc(graph: ^Graph($T)) {
	for node in &graph.nodes {
		free_node(node)
	}
	delete(graph.nodes)
	delete(graph.roots)
	queue.destroy(&graph._trav)
}

consume :: proc(dest: ^Graph($T), src: ^Graph(T)) {
	append(&dest.nodes, ..src.nodes[:])
	clear(&src.nodes)
}

add_node :: proc(graph: ^Graph($T), node: ^Node(T)) {
	append(&graph.nodes, node)
}

add_data :: proc(graph: ^Graph($T), val: T) -> ^Node(T) {
	new_node := new_node(val)
	add_node(graph, new_node)
	return new_node
}

add :: proc {add_data, add_node}

remove :: proc(graph: ^Graph($T), node: ^Node(T)) -> T {
	remove_idx: int

	for n, i in graph.nodes {
		if n == node {
			remove_idx = i
		} else if n.out[0] == node {
			n.out[0] = nil
		} else if n.out[1] == node {
			n.out[1] = nil
		}
	}

	unordered_remove(&graph.nodes, remove_idx)
	return free_node(node)
}

/* breadth first */
traverse :: proc(graph: ^Graph($T)) -> ^Node(T) {
	if graph._root_idx == -1 {
		reset(graph)
	}

	for graph._trav.len == 0 {
		if graph._root_idx >= len(graph.roots) {
			return nil
		}

		next := graph.roots[graph._root_idx]
		graph._root_idx += 1

		queue.push(&graph._trav, next)
		return traverse(graph)
	}

	/* In case of cycle */
	next := queue.pop_front(&graph._trav)
	for next.visit_count > 0 && graph._trav.len > 0 {
		next = queue.pop_front(&graph._trav)
	}
	if next.visit_count > 0 {
		return traverse(graph)
	}

	next.visit_count += 1
	if next.out[0] != nil && next.out[0].visit_count == 0 {
		queue.push(&graph._trav, next.out[0])
	}
	if next.out[1] != nil && next.out[1].visit_count == 0 {
		queue.push(&graph._trav, next.out[1])
	}
	
	return next
}

/* After traverse returns nil, you will need this to reset the
 * graph for traversal again.
 */
reset :: proc(graph: ^Graph($T)) {
	if graph._root_idx == -1 {
		set_roots(graph)
	}
	if len(graph.roots) == 0 {
		return
	}

	queue.clear(&graph._trav)
	graph._root_idx = 1

	for n in &graph.nodes {
		n.visit_count = 0
	}

	/* Start by pushing the first root */
	queue.push(&graph._trav, graph.roots[0])
}

/* Ignore the is_root flag and attempt to derive the roots
 * from the way the graph is laid out.
 */
derive_roots :: proc(graph: ^Graph($T)) {
	clear(&graph.roots)
	for n in &graph.nodes {
		n.is_root = false
	}
	_assume_roots(graph)
}

/* If you have manually set the roots (via is_root),
 * then use this proc to let the graph know about them.
 * Otherwise, act just like derive_roots.
 *
 * NOTE: _assume_roots will set the node.is_root flag
 *       Running this proc a second time will not reset
 *       the roots from the first run. Use derive_roots
 *       for that.
 */
set_roots :: proc(graph: ^Graph($T)) {
	clear(&graph.roots)
	for n in graph.nodes {
		if n.is_root {
			append(&graph.roots, n)
		}
	}

	/* if no roots found, just assume */
	if len(graph.roots) == 0 {
		_assume_roots(graph)
	}
}

@private
_assume_roots :: proc(graph: ^Graph($T)) {
	if len(graph.nodes) == 0 {
		return
	}

	for n in &graph.nodes {
		n.visit_count = 0
	}

	for n in &graph.nodes {
		if n.out[0] != nil {
			n.out[0].visit_count += 1
		}
		if n.out[1] != nil {
			n.out[1].visit_count += 1
		}
	}

	for n in &graph.nodes {
		if n.visit_count == 0 {
			n.is_root = true
			append(&graph.roots, n)
		}
	}
}
