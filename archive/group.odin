package streamql

Group :: struct {
	expressions: [dynamic]Expression,
}

new_group :: proc() -> ^Group {
	g := new(Group)
	g^ = {
		expressions = make([dynamic]Expression),
	}
	return g
}

group_add_expression :: proc(g: ^Group, expr: ^Expression) -> ^Expression {
	append(&g.expressions, expr^)
	return &g.expressions[len(g.expressions) - 1]
}
