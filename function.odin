package streamql

Function :: struct {
	args: [dynamic]Expression,
}

function_add_expression :: proc(fn: ^Function, expr: ^Expression) {
	append(&fn.args, expr^)
}
