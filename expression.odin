package streamql

import "core:reflect"
import "core:strings"
import "core:strconv"

Expression_Props :: enum u8 {
	Is_Passthrough,
	Descending,
}

E_Column_Name :: distinct string
E_Full_Record :: distinct string
E_Row_Number ::  distinct i64
E_Reference :: distinct ^Expression
E_Asterisk :: distinct string
E_Constant :: distinct Data
E_Grouping :: distinct ^Expression
E_Subquery :: distinct ^Query
E_Variable :: distinct i64
E_Null :: distinct string

Expression_Data :: union {
	E_Column_Name,
	E_Full_Record,
	E_Row_Number,
	E_Reference,
	E_Asterisk,
	E_Constant,
	E_Grouping,
	E_Subquery,
	E_Variable,
	E_Null,
	Case,
	Function,
	Aggregate,
}

Expression :: struct {
	buf: strings.Builder,
	alias: string,
	table_name: string,
	data: Expression_Data,
	props: bit_set[Expression_Props],
}

make_expression_const_i :: proc(val: i64) -> Expression {
	return Expression {
		data = E_Constant(val),
	}
}

make_expression_const_f :: proc(val: f64) -> Expression {
	return Expression {
		data = E_Constant(val),
	}
}

make_expression_const_s :: proc(s: string) -> Expression {
	return Expression {
		data = E_Constant(strings.clone(s)),
	}
}

make_expression_subquery :: proc(subquery: ^Query) -> Expression {
	return Expression {
		data = E_Subquery(subquery),
	}
}

make_expression_name :: proc(name: string, table_name: string) -> Expression {
	expr : Expression = {
		data = E_Column_Name(strings.clone(name)),
		alias = strings.clone(name),
		table_name = table_name,
	}
	switch name {
	case "__ROWNUM":
		expr.data = E_Row_Number(0)
	case "__REC":
		expr.data = E_Full_Record("")
	case "__CR":
		expr.data = E_Constant(string("\r"))
	case "__LF":
		expr.data = E_Constant(string("\n"))
	case "__CRLF":
		expr.data = E_Constant(string("\r\n"))
	}
	return expr
}

make_expression_agg :: proc(agg: Aggregate) -> Expression {
	return Expression {
		data = agg,
	}
}

make_expression_fn :: proc(fn: Function) -> Expression {
	return Expression {
		data = fn,
	}
}

make_expression_null :: proc(null: E_Null) -> Expression {
	return Expression {
		data = null,
	}
}

make_expression_case :: proc(c: Case) -> Expression {
	return Expression {
		data = c,
	}
}

make_expression_asterisk :: proc(aster: E_Asterisk) -> Expression {
	return Expression {
		data = aster,
	}
}

make_expression_var :: proc(var: E_Variable) -> Expression {
	return Expression {
		data = var,
	}
}

make_expression :: proc{
	make_expression_const_i,
	make_expression_const_f,
	make_expression_const_s,
	make_expression_subquery,
	make_expression_name,
	make_expression_agg,
	make_expression_fn,
	make_expression_null,
	make_expression_asterisk,
	make_expression_case,
	make_expression_var,
}

expression_cat_description :: proc(expr: ^Expression, b: ^strings.Builder) {
	switch v in expr.data {
	case E_Grouping:
		strings.write_string(b, expr.alias)
	case E_Column_Name:
		strings.write_string(b, expr.alias)
	case E_Full_Record:
		strings.write_string(b, expr.alias)
	case E_Row_Number:
		strings.write_string(b, expr.alias)
	case E_Reference:
		strings.write_string(b, expr.alias)
	case E_Asterisk:
		strings.write_byte(b, '*')
	case E_Constant:
		switch c in expr.data.(E_Constant) {
		case i64:
			strings.write_i64(b, c)
		case f64:
			buf: [64]u8
			str := strconv.ftoa(buf[:], c, 'f', 3, 64)
			strings.write_string(b, str)
		case string:
			strings.write_string(b, c)
		}
	case E_Subquery:
		strings.write_string(b, "<subquery>")
	case E_Variable:
		strings.write_string(b, "var<")
		strings.write_i64(b, i64(v))
		strings.write_byte(b, '>')
	case E_Null:
		strings.write_string(b, "NULL")
	case Case:
		strings.write_string(b, "[case expr]")
	case Function:
		fn := expr.data.(Function)
		fn_names := reflect.enum_field_names(typeid_of(Function_Type))
		strings.write_string(b, fn_names[fn.type])
		strings.write_byte(b, '(')

		first := true
		for e in &fn.args {
			if !first {
				strings.write_byte(b, ',')
			}
			first = false
			expression_cat_description(&e, b)
		}
		strings.write_byte(b, ')')
	case Aggregate:
		strings.write_string(b, "[aggregate]")
	}
}





