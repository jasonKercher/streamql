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

	/* Functions - should map to Token_Type */
	Abs = 500,
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

Function :: struct {
	args: [dynamic]Expression,
	min_args: u16,
	max_args: u16,
	type: Function_Type,
}

make_function :: proc(fn_type: Function_Type) -> Function {
	return Function {
		type = fn_type,
	}
}

function_add_expression :: proc(fn: ^Function, expr: ^Expression) -> ^Expression {
	if cap(fn.args) == 0 {
		fn.args = make([dynamic]Expression)
	}
	append(&fn.args, expr^)
	return &fn.args[len(fn.args) - 1]
}
