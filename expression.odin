package streamql

import "core:strings"

Expression_Props :: enum u8 {
	Is_Passthrough,
	Descending,
}

Expression_Type :: enum u8 {
	Undefined,
	Column_Name,
	Full_Record,
	Row_Number,
	Aggregate,
	Reference,
	Asterisk,
	Constant,
	Function,
	Grouping,
	Subquery,
	Variable,
	Case,
	Null,
}

Expression_Data :: struct #raw_union {
	i: i64,
	f: f64,
	s: string, /* constant or name */
	q: ^Query,
	fn: Function,
	agg: Aggregate,
	c: Case,
}

Expression :: struct {
	buf: strings.Builder,
	alias: string,
	table_name: string,
	data: Expression_Data,
	type: Expression_Type,
	props: bit_set[Expression_Props],
}

make_expression_i :: proc(val: i64) -> Expression {
	return Expression {
		type = .Constant,
		data = { i = val },
	}
}

make_expression_f :: proc(val: f64) -> Expression {
	return Expression {
		type = .Constant,
		data = { f =  val },
	}
}

make_expression_s :: proc(s: string) -> Expression {
	return Expression {
		type = .Constant,
		data = { s = strings.clone(s) },
	}
}

make_expression_subquery :: proc(subquery: ^Query) -> Expression {
	return Expression {
		type = .Subquery,
		data = { q = subquery },
	}
}

make_expression_name :: proc(name: string, table_name: string) -> Expression {
	expr : Expression = {
		type = .Column_Name,
		data = { s = strings.clone(name) },
		alias = strings.clone(name),
		table_name = table_name,
	}
	switch name {
	case "__ROWNUM":
		expr.type = .Row_Number
	case "__REC":
		expr.type = .Full_Record
	case "__CR":
		expr.data.s = "\r"
	case "__LF":
		expr.data.s = "\n"
	case "__CRLF":
		expr.data.s = "\r\n"
	}
	return expr
}

make_expression_agg :: proc(agg: Aggregate) -> Expression {
	return Expression {
		type = .Aggregate,
		data = { agg = agg },
	}
}

make_expression_fn :: proc(fn: Function) -> Expression {
	return Expression {
		type = .Function,
		data = { fn = fn },
	}
}

make_expression_special :: proc(type: Expression_Type) -> Expression {
	return Expression {
		type = type,
	}
}

make_expression :: proc{
	make_expression_i,
	make_expression_f,
	make_expression_s,
	make_expression_subquery,
	make_expression_name,
	make_expression_agg,
	make_expression_fn,
	make_expression_special,
}
