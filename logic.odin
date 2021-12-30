package streamql

Logic_Group_Type :: enum {
	And,
	Or,
	Predicate,
	Predicate_Negated,
}

Logic_Group :: struct {
	items: [2]^Logic_Group,
	//expressions: [dynamic]Expression,
	joinable: [dynamic]^Logic_Group,
	join_logic: ^Logic,
	condition: ^Logic,
	type: Logic_Group_Type,
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
	Sub_In,
	Like,
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

logic_add_expression :: proc(l: ^Logic, expr: ^Expression) {
	if l.exprs[0].type == .Undefined {
		l.exprs[0] = expr^
		return
	}
	l.exprs[1] = expr^
}
