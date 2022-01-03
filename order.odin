package streamql

Order :: struct {
	expressions: [dynamic]Expression,
}

order_add_expression :: proc(o: ^Order, expr: ^Expression) -> ^Expression {
	append(&o.expressions, expr^)
	return &o.expressions[len(o.expressions) - 1]
}
