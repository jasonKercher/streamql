package streamql

enum Logic_Group_Type {
	And,
	Or,
	Predicate,
	Predicate_Negated,
}

Logic_Group :: struct {
	items: [2]Logic_Group,
	//expressions: [dynamic]Expression,
	joinable: [dynamic],
	join_logic: ^Logic,
	condition: ^Logic,
	type: Logic_Group_Type,
}


enum Comparison {
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
	data_type: Field_Type,
}

new_logic :: proc() -> ^Logic {
	l := new(logic)
	l = {
		comp_type = .None
	}
	return l
}

logic_add_expression(l: ^Logic, expr: ^Expression) {
	if l.exprs[0].type == .Undefined {
		l.exprs[0] = expr^
		return
	}
	l.exprs[1] = expr^
}
