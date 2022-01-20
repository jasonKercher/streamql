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

make_plan :: proc() -> Plan {
	p := Plan {
		proc_graph = bigraph.make_graph(Process),
		op_true = bigraph.new_node(make_process(nil, "OP_TRUE")),
		op_false = bigraph.new_node(make_process(nil, "OP_FALSE")),
		curr = bigraph.new_node(make_process(nil, "START")),
	}
	p.curr.data.props += {.Is_Passive}
	return p
}

destroy_plan :: proc(p: ^Plan) {
	bigraph.destroy(&p.proc_graph)
}

plan_build :: proc(sql: ^Streamql) -> Result {
	for q in &sql.queries {
		_build(sql, q) or_return
	}
	return .Ok
}

@private
_make_join_proc :: proc(p: ^Plan, type: Join_Type, algo_str: string) -> Process {
	msg: string
	#partial switch type {
	case .Inner:
		msg = fmt.tprintf("INNER JOIN (%s)", algo_str)
	case .Left:
		msg = fmt.tprintf("LEFT JOIN (%s)", algo_str)
	case .Right:
		msg = fmt.tprintf("RIGHT JOIN (%s)", algo_str)
	case .Full:
		msg = fmt.tprintf("FULL JOIN (%s)", algo_str)
	case .Cross:
		msg = fmt.tprintf("CROSS JOIN (%s)", algo_str)
	}

	return make_process(p, msg)
}

@private
_subquery_inlist :: proc(p: ^Plan, lg: ^Logic_Group, logic_proc: ^Process) -> Result {
	return not_implemented()
}

@private
_check_for_special_expr :: proc(p: ^Plan, process: ^Process, expr: ^Expression) {
	#partial switch v in &expr.data {
	case Expr_Subquery:
		process_add_to_wait_list(process, &v.plan.op_true.data)
	case Expr_Case:
		not_implemented()
	case Expr_Aggregate:
		_check_for_special_exprs(p, process, &v.args)
	case Expr_Function:
		_check_for_special_exprs(p, process, &v.args)
	}
}

@private
_check_for_special_exprs :: proc(p: ^Plan, process: ^Process, exprs: ^[dynamic]Expression) {
	exprs := exprs
	for e in exprs {
		_check_for_special_expr(p, process, &e)
	}
}

_check_for_special :: proc{_check_for_special_expr, _check_for_special_exprs}

@private
_logic_to_process :: proc(p: ^Plan, logic_proc: ^Process, lg: ^Logic_Group, sb: ^strings.Builder) -> Result {
	switch lg.type {
	case .And:
		strings.write_string(sb, "AND(")
	case .Or:
		strings.write_string(sb, "OR(")
	case .Not:
		strings.write_string(sb, "NOT(")
	case .Predicate_Negated:
		strings.write_string(sb, "NOT ")
		fallthrough
	case .Predicate:
		if int(lg.condition.comp_type) >= int(Comparison.Sub_In) {
			_subquery_inlist(p, lg, logic_proc) or_return
		}
		_check_for_special(p, logic_proc, &lg.condition.exprs[0])
		_check_for_special(p, logic_proc, &lg.condition.exprs[1])
		logic_assign_process(lg.condition, logic_proc) or_return
	}

	if lg.items[0] != nil {
		_logic_to_process(p, logic_proc, lg.items[0], sb) or_return
	}
	if lg.items[1] != nil {
		_logic_to_process(p, logic_proc, lg.items[1], sb) or_return
	}

	strings.write_byte(sb, ')')
	return .Ok
}

@private
_insert_logic_proc :: proc(p: ^Plan, lg: ^Logic_Group, is_hash_join: bool = false) -> (ptr: ^Process, res: Result) {
	logic_proc := make_process(p, "")
	logic_proc.action__ = sql_logic
	logic_proc.data = lg

	sb := strings.make_builder()
	_logic_to_process(p, &logic_proc, lg, &sb) or_return
	logic_proc.msg = strings.to_string(sb)

	logic_node := bigraph.add(&p.proc_graph, logic_proc)
	
	if is_hash_join {
		p.curr.out[1] = logic_node
		if logic_group_get_condition_count(lg) == 1 {
			logic_node.data.props += {.Is_Passive}
		}
	} else {
		p.curr.out[0] = logic_node
	}

	logic_node.out[0] = p.op_false

	logic_true_proc := make_process(nil, "logic true")
	logic_true_proc.props += {.Is_Passive}
	logic_true_node := bigraph.add(&p.proc_graph, logic_true_proc)
	logic_node.out[1] = logic_true_node
	p.curr = logic_true_node

	ptr = &logic_node.data
	return
}

@private
_from :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if len(q.sources) == 0 {
		return .Ok
	}

	from_node: ^bigraph.Node(Process)

	switch v in &q.sources[0].data {
	case string: /* File Source */
		msg := fmt.tprintf("%s: stream read", v)
		from_proc := make_process(&q.plan, msg)
		from_proc.props += {.Root_Fifo0}
		from_proc.action__ = sql_read
		from_proc.data = &q.sources[0]
		from_node := bigraph.add(&q.plan.proc_graph, from_proc)
		from_node.is_root = true
	case ^Query: /* Subquery Source */
		from_proc := make_process(&q.plan, "subquery source1")
		from_proc.action__ = sql_read
		from_proc.data = &q.sources[0]
		from_node := bigraph.add(&q.plan.proc_graph, from_proc)
		_build(sql, q, from_node) or_return
		bigraph.consume(&q.plan.proc_graph, &v.plan.proc_graph)
		destroy_plan(&v.plan)
	}
	/* NOTE: This next line may be incorrect */
	//q.sources[0].read_proc = from_proc

	q.plan.curr.out[0] = from_node
	q.plan.curr = from_node

	for src, i in &q.sources {
		if i == 0 {
			continue
		}
		join_proc: Process
		join_node: ^bigraph.Node(Process)
		is_hash_join := src.join_data != nil
		if is_hash_join {
			join_proc = _make_join_proc(&q.plan, src.join_type, "hash")
			join_proc.action__ = sql_hash_join
			join_proc.props += {.Has_Second_Input}
			join_proc.data = &src

			if .Is_Stdin in src.props {
				reader_start_file_backed_input(&src.schema.reader)
			}
			hash_join_init(&src)
			
			read_node: ^bigraph.Node(Process)
			switch v in &src.data {
			case string: /* File Source */
				msg: string
				if .Is_Stdin in src.props {
					msg = "file-backed read stdin"
				} else {
					msg = fmt.tprintf("%s: random access", v)
				}
				read_proc := make_process(&q.plan, msg)
				read_proc.props += {.Root_Fifo0, .Is_Secondary}
				read_proc.action__ = sql_read
				read_proc.data = &src
				read_node = bigraph.add(&q.plan.proc_graph, read_proc)
				read_node.is_root = true
			case ^Query: /* Subquery */
				subquery_start_file_backed_input(&src.schema.reader)
				read_proc := make_process(&q.plan, "file-backed read subquery")
				read_proc.props += {.Is_Secondary}
				read_proc.action__ = sql_read
				read_proc.data = &src
				read_node = bigraph.add(&q.plan.proc_graph, read_proc)
				_build(sql, v, read_node) or_return
				bigraph.consume(&q.plan.proc_graph, &v.plan.proc_graph)
				// destroy_plan(&v.plan)
			}

			join_node := bigraph.add(&q.plan.proc_graph, join_proc)
			read_node.out[0] = join_node
		} else { /* Cartesian */
			if src.join_type != .Inner {
				fmt.fprintf(os.stderr, "cartesian join only works with INNER JOIN\n")
				return .Error
			}
			if _, is_subquery := src.data.(^Query); is_subquery {
				fmt.fprintf(os.stderr, "subquery invalid on the right side of cartesian join\n")
				return .Error
			}
			fmt.fprintf(os.stderr, "Warning: slow cartesian join detected\n")
			join_proc := _make_join_proc(&q.plan, src.join_type, "cartesian")
			join_proc.props += {.Root_Fifo1}
			join_proc.data = &src
			join_node := bigraph.add(&q.plan.proc_graph, join_proc)
			join_node.is_root = true
		}

		q.plan.curr.out[0] = join_node
		q.plan.curr = join_node

		if src.join_logic != nil {
			logic_proc := _insert_logic_proc(&q.plan, src.join_logic, is_hash_join) or_return
			if src.join_type == .Left {
				logic_proc.action__ = sql_left_join_logic
			}
		}
	}
	return .Ok
}

@private
_where :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if q.where_ == nil {
		return .Ok
	}
	_, res := _insert_logic_proc(&q.plan, q.where_)
	return res
}

@private
_group :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if q.groupby == nil {
		return .Ok
	}
	return not_implemented()
}

@private
_having :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if q.having == nil {
		return .Ok
	}
	_, res := _insert_logic_proc(&q.plan, q.having)
	return res
}

@private
_operation :: proc(sql: ^Streamql, q: ^Query, entry: ^bigraph.Node(Process), is_union: bool = false) -> Result {
	prev := q.plan.curr
	prev.out[0] = q.plan.op_true
	q.plan.curr = q.plan.op_true

	/* Current no longer matters. After operation, we
	 * do order where current DOES matter... BUT
	 * if we are in a union we should not encounter
	 * ORDER BY...
	 */
	if is_union {
		q.plan.op_false.data.props += {.Is_Passive}
		if .Is_Passive in prev.data.props {
			q.plan.curr = prev
			return .Ok
		}
	}

	op_apply_process(q, is_union || entry != nil) or_return
	_check_for_special(&q.plan, &q.plan.op_true.data, op_get_expressions(&q.operation))

	op_add_exprs := op_get_additional_expressions(&q.operation)
	_check_for_special(&q.plan, &q.plan.op_true.data, op_add_exprs)

	if entry != nil {
		q.plan.op_true.out[0] = entry
	}
	return .Ok
}

@private
_union :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if len(q.unions) == 0 {
		return .Ok
	}
	return not_implemented()
}

@private
_order :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if q.orderby == nil {
		return .Ok
	}
	return not_implemented()
}

@private
_clear_passive :: proc(p: ^Plan) {
}

@private
_stranded_roots_for_delete :: proc(p: ^Plan) {
}

@private
_mark_roots_const :: proc(roots: ^[dynamic]^bigraph.Node(Process), id: u8) {
}

@private
_all_roots_are_const :: proc(roots: ^[dynamic]^bigraph.Node(Process)) -> bool {
	return false
}

@private
_search_and_mark_const_selects :: proc(q: ^Query) {
}


@private
_activate_procs :: proc(sql: ^Streamql, q: ^Query) {
}

@private
_make_pipes :: proc(p: ^Plan) {
}

@private
_update_pipes :: proc(g: ^bigraph.Graph(Process)) {
}

@private
_calculate_execution_order :: proc(p: ^Plan) {
}

@private
_build :: proc(sql: ^Streamql, q: ^Query, entry: ^bigraph.Node(Process) = nil, is_union: bool = false) -> Result {
	for subq in &q.subquery_exprs {
		_build(sql, subq) or_return
	}

	q.plan = make_plan()

	_from(sql, q) or_return
	_where(sql, q) or_return
	_group(sql, q) or_return
	_having(sql, q) or_return
	_operation(sql, q, entry, is_union) or_return

	//_print(&q.plan)

	_clear_passive(&q.plan)
	bigraph.set_roots(&q.plan.proc_graph)
	_union(sql, q) or_return
	_order(sql, q) or_return
	_clear_passive(&q.plan)
	bigraph.set_roots(&q.plan.proc_graph)

	_stranded_roots_for_delete(&q.plan)

	if len(q.plan.proc_graph.nodes) == 0 {
		if entry != nil {
			entry.data.props += {.Is_Const}
		}
		return .Ok
	}

	for subq in &q.subquery_exprs {
		bigraph.consume(&q.plan.proc_graph, &subq.plan.proc_graph)
		//destroy_plan(&subq.plan)
	}

	bigraph.set_roots(&q.plan.proc_graph)
	if len(q.sources) == 0 {
		_mark_roots_const(&q.plan.proc_graph.roots, q.plan.id)
	}

	if _all_roots_are_const(&q.plan.proc_graph.roots) {
		q.plan.state += {.Is_Const}
	}
	_search_and_mark_const_selects(q)

	/* Only non-subqueries beyond this point */
	if q.sub_id != 0 {
		return .Ok
	}

	_activate_procs(sql, q)
	_make_pipes(&q.plan)
	_update_pipes(&q.plan.proc_graph)
	_calculate_execution_order(&q.plan)

	return .Ok
}
