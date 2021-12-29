package streamql

Order :: struct {
	expressions: [dynamic]Expression,
}

order_add_expression :: proc(g: ^Order, expr: ^Expression) {
	append(&g.expressions, expr^)
}
