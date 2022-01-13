package streamql

Logic_Group_Type :: enum {
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

new_logic_group :: proc(type: Logic_Group_Type) -> ^Logic_Group {
	lg := new(Logic_Group)
	return lg
}

Comparison :: enum {
	None = -3,
	False,
	True,
	Eq,
	Ne,
	Gt,
	Ge,
	Lt,
	Le,
	In,
	Not_In,
	Sub_In,
	Like,
	Not_Like,
	Null,
}

Logic :: struct {
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

logic_must_be_true :: proc(lg: ^Logic_Group, l: ^Logic) -> bool {
	return false
}
