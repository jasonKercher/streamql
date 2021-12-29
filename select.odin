package streamql

Select :: struct {
	expressions: [dynamic]Expression,
}

make_select :: proc() -> Select {
	return Select {
		expressions = make([dynamic]Expression),
	}
}

select_add_expression :: proc(s: ^Select, expr: ^Expression) {
	append(&s.expressions, expr^)
}
