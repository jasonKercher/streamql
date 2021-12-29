package streamql

Group :: struct {
	expressions: [dynamic]Expression,
}

group_add_expression :: proc(g: ^Group, expr: ^Expression) {
	append(&g.expressions, expr^)
}
