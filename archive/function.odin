package streamql

/* Copied from */
Function_Type :: enum {
	/* Standard Math */
	Plus,
	Minus,
	Multiply,
	Divide,
	Modulus,
	Bit_Or,
	Bit_And,
	Bit_Xor,

	/* Unary */
	Plus_Unary,
	Minus_Unary,
	Bit_Not_Unary,

	/* Functions */
	Abs,
	Ascii,
	Cast,
	Ceiling,
	Char,
	Charindex,
	Coalesce,
	Concat,
	Convert,
	Datalength,
	Dateadd,
	Datediff,
	Datename,
	Datepart,
	Floor,
	Getdate,
	Getutcdate,
	Isdate,
	Isnull,
	Isnumeric,
	Left,
	Len,
	Lower,
	Ltrim,
	Nchar,
	Nullif,
	Patindex,
	Rand,
	Replace,
	Right,
	Round,
	Rtrim,
	Space,
	Stuff,
	Substring,
	Try_Cast,
	Upper,
	User_Name,
}

Function_Call :: proc(fn_expr: ^Expression) -> Result

Expr_Function :: struct {
	call__: Function_Call,
	args: [dynamic]Expression,
	min_args: u16,
	max_args: u16,
	type: Function_Type,
}

make_function :: proc(fn_type: Function_Type) -> Expr_Function {
	return Expr_Function {
		type = fn_type,
	}
}

destroy_function :: proc(fn: ^Expr_Function) {
	if fn == nil {
		return
	}
	delete(fn.args)
}

function_add_expression :: proc(fn: ^Expr_Function, expr: ^Expression) -> ^Expression {
	if cap(fn.args) == 0 {
		fn.args = make([dynamic]Expression)
	}
	append(&fn.args, expr^)
	return &fn.args[len(fn.args) - 1]
}

function_op_resolve :: proc(fn: ^Expr_Function) -> Result {
	return not_implemented()
}

function_validate :: proc(fn: ^Expr_Function, expr: ^Expression) -> Result {
	return not_implemented()
}
