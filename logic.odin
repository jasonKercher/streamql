//+private
package streamql

import "core:strings"
import "bytemap"

Logic_Group_Type :: enum {
	_Parent,
	And,
	Or,
	Not,
	Predicate,
	Predicate_Negated,
}

Logic_Group :: struct {
	items: [2]^Logic_Group,
	joinable: [dynamic]^Logic_Group,
	join_logic: ^Logic,
	condition: ^Logic,
	type: Logic_Group_Type,
}

init_logic_group :: proc(lg: ^Logic_Group, type: Logic_Group_Type, op_str: string) {
	lg.type = type
	if type != .Predicate && type != .Predicate_Negated {
		return
	}

	lg.condition = new_logic()

	assert(op_str != "")
	switch op_str {
	case "=":
		lg.condition.comp_type = .Eq
	case "!=", "<>":
		lg.condition.comp_type = .Ne
	case ">":
		lg.condition.comp_type = .Gt
	case ">=":
		lg.condition.comp_type = .Ge
	case "<":
		lg.condition.comp_type = .Lt
	case "<=":
		lg.condition.comp_type = .Le
	case "rn":
		lg.condition.comp_type = .In
	case "Sub_In":
		lg.condition.comp_type = .Sub_In
	case "Is_Null":
		lg.condition.comp_type = .Is_Null
	case "Like":
		lg.condition.comp_type = .Like
	}
}

new_logic_group :: proc(type: Logic_Group_Type, op_str: string = "") -> ^Logic_Group {
	lg := new(Logic_Group)
	init_logic_group(lg, type, op_str)
	return lg
}

free_logic_group :: proc(lg: ^Logic_Group) {
	free(lg)
}

logic_group_get_condition_count :: proc(lg: ^Logic_Group) -> int {
	not_implemented()
	return -1
}

LOGIC_COUNT :: 10

Comparison :: enum {
	None = -3,
	False = -2,
	True = -1,
	Eq = 0,
	Ne,
	Gt,
	Ge,
	Lt,
	Le,
	In,
	Sub_In,
	Is_Null,
	Like,
}

_comp_strs : [LOGIC_COUNT]string = {
	" = ",
	" != ",
	" > ",
	" >= ",
	" < ",
	" <= ",
	" IN ",
	" IN(select...) ",
	" IS NULL ",
	" LIKE ",
}
Logic_Call :: proc(l: ^Logic, recs: ^Record) -> (bool, Result)

Logic_Data :: union {
	^In_List,
	^Like,
}

Logic :: struct {
	logic__: Logic_Call,
	data: Logic_Data,
	exprs: [2]Expression,
	comp_type: Comparison,
	data_type: Data_Type,
}

new_logic :: proc() -> ^Logic {
	l := new(Logic)
	l^ = {
		comp_type = .None,
	}
	return l
}

logic_add_expression :: proc(l: ^Logic, expr: ^Expression) -> ^Expression {
	if l.exprs[0].data == nil {
		l.exprs[0] = expr^
		return &l.exprs[0]
	}
	l.exprs[1] = expr^
	return &l.exprs[1]
}

logic_assign_process :: proc(l: ^Logic, logic_proc: ^Process) -> Result {
	assert(int(l.comp_type) >= 0)
	sb := strings.make_builder()
	expression_cat_description(&l.exprs[0], &sb)
	strings.write_string(&sb, _comp_strs[l.comp_type])

	#partial switch l.comp_type {
	case .In, .Sub_In:
		/* TODO: + inlist description */
		return not_implemented()
	case .Is_Null:
		l.data_type = .String
	case .Like:
		l.data_type = .String
		fallthrough
	case:
		expression_cat_description(&l.exprs[1], &sb)
		l.data_type = data_determine_type(l.exprs[0].data_type, l.exprs[1].data_type)
	}

	l.logic__ = _logic_procs[l.comp_type][l.data_type]

	return .Ok
}

logic_must_be_true :: proc(lg: ^Logic_Group, l: ^Logic) -> bool {
	not_implemented()
	return false
}

In_Data :: union {
	^Query,
	[dynamic]Expression,
}

In_List :: struct {
	data: In_Data,
	list: bytemap.Set,
	return_state: int,
}

Like :: struct {
	str: string,
}

_logic_procs : [LOGIC_COUNT][DATA_TYPE_COUNT] Logic_Call = {
        {sql_logic_eq_i,    sql_logic_eq_f,    sql_logic_eq_s   },
        {sql_logic_ne_i,    sql_logic_ne_f,    sql_logic_ne_s   },
        {sql_logic_gt_i,    sql_logic_gt_f,    sql_logic_gt_s   },
        {sql_logic_ge_i,    sql_logic_ge_f,    sql_logic_ge_s   },
        {sql_logic_lt_i,    sql_logic_lt_f,    sql_logic_lt_s   },
        {sql_logic_le_i,    sql_logic_le_f,    sql_logic_le_s   },
        {sql_logic_in_i,    sql_logic_in_f,    sql_logic_in_s   },
        {sql_logic_subin_i, sql_logic_subin_f, sql_logic_subin_s},
        {sql_logic_is_null, sql_logic_is_null, sql_logic_is_null},
        {nil,               nil,               sql_logic_like   },
}
