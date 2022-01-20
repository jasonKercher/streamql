package streamql

import "core:reflect"
import "core:strings"
import "core:strconv"

Expression_Props :: enum u8 {
	Is_Passthrough,
	Descending,
}

Expr_Full_Record :: distinct i32
Expr_Row_Number ::  distinct i64
Expr_Reference :: distinct ^Expression
Expr_Asterisk :: distinct i32
Expr_Constant :: distinct Data
Expr_Grouping :: distinct ^Expression
Expr_Subquery :: distinct ^Query
Expr_Variable :: distinct i64
Expr_Null :: distinct string

Expr_Column_Name :: struct {
	name: string,
	src_idx: i32,
	col_idx: i32,
}

Expression_Data :: union {
	Expr_Column_Name,
	Expr_Full_Record,
	Expr_Row_Number,
	Expr_Reference,
	Expr_Asterisk,
	Expr_Constant,
	Expr_Grouping,
	Expr_Subquery,
	Expr_Variable,
	Expr_Null,
	Expr_Case,
	Expr_Function,
	Expr_Aggregate,
}

Expression :: struct {
	buf: strings.Builder,
	fn_bak: ^Expr_Function,
	alias: string,
	table_name: string,
	data: Expression_Data,
	props: bit_set[Expression_Props],
	data_type: Data_Type,
	subq_idx: u16,
}

make_expression_const_i :: proc(val: i64) -> Expression {
	return Expression {
		data = Expr_Constant(val),
	}
}

make_expression_const_f :: proc(val: f64) -> Expression {
	return Expression {
		data = Expr_Constant(val),
	}
}

make_expression_const_s :: proc(s: string) -> Expression {
	return Expression {
		data = Expr_Constant(strings.clone(s)),
	}
}

make_expression_subquery :: proc(subquery: ^Query) -> Expression {
	return Expression {
		data = Expr_Subquery(subquery),
	}
}

make_expression_name :: proc(name, table_name: string) -> Expression {
	expr : Expression = {
		alias = strings.clone(name),
		table_name = table_name,
	}
	switch name {
	case "__ROWNUM":
		expr.data = Expr_Row_Number(0)
	case "__REC":
		expr.data = Expr_Full_Record(-1)
	case "__CR":
		expr.data = Expr_Constant(string("\r"))
	case "__LF":
		expr.data = Expr_Constant(string("\n"))
	case "__CRLF":
		expr.data = Expr_Constant(string("\r\n"))
	case:
		expr.data = Expr_Column_Name { name = strings.clone(name), col_idx = -1 }
	}
	return expr
}

make_expression_agg :: proc(agg: Expr_Aggregate) -> Expression {
	return Expression {
		data = agg,
	}
}

make_expression_fn :: proc(fn: Expr_Function) -> Expression {
	return Expression {
		data = fn,
	}
}

make_expression_null :: proc(null: Expr_Null) -> Expression {
	return Expression {
		data = null,
	}
}

make_expression_case :: proc(c: Expr_Case) -> Expression {
	return Expression {
		data = c,
	}
}

make_expression_asterisk :: proc(aster: Expr_Asterisk) -> Expression {
	return Expression {
		data = aster,
	}
}

make_expression_var :: proc(var: Expr_Variable) -> Expression {
	return Expression {
		data = var,
	}
}

make_expression_ref :: proc(expr: Expr_Reference) -> Expression {
	return Expression {
		data = expr,
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
	make_expression_ref,
}

destroy_expression :: proc(expr: ^Expression) {
	strings.destroy_builder(&expr.buf)
	destroy_function(expr.fn_bak)
	free(expr.fn_bak)
}

expression_cat_description :: proc(expr: ^Expression, b: ^strings.Builder) {
	switch v in expr.data {
	case Expr_Grouping:
		strings.write_string(b, expr.alias)
	case Expr_Column_Name:
		strings.write_string(b, expr.alias)
	case Expr_Full_Record:
		strings.write_string(b, expr.alias)
	case Expr_Row_Number:
		strings.write_string(b, expr.alias)
	case Expr_Reference:
		strings.write_string(b, expr.alias)
	case Expr_Asterisk:
		strings.write_byte(b, '*')
	case Expr_Constant:
		switch c in expr.data.(Expr_Constant) {
		case i64:
			strings.write_i64(b, c)
		case f64:
			buf: [64]u8
			str := strconv.ftoa(buf[:], c, 'f', 3, 64)
			strings.write_string(b, str)
		case string:
			strings.write_string(b, c)
		}
	case Expr_Subquery:
		strings.write_string(b, "<subquery>")
	case Expr_Variable:
		strings.write_string(b, "var<")
		strings.write_i64(b, i64(v))
		strings.write_byte(b, '>')
	case Expr_Null:
		strings.write_string(b, "NULL")
	case Expr_Case:
		strings.write_string(b, "[case expr]")
	case Expr_Function:
		fn := expr.data.(Expr_Function)
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
	case Expr_Aggregate:
		strings.write_string(b, "[aggregate]")
	}
}

expression_get_int :: proc(expr: ^Expression, recs: []Record = nil) -> (i64, Result) {
	return 0, .Ok
}

expression_get_float :: proc(expr: ^Expression, recs: []Record = nil) -> (f64, Result) {
	return 0, .Ok
}

expression_get_string :: proc(expr: ^Expression, recs: []Record = nil) -> (string, Result) {
	return "", .Ok
}

//expression_update_indicies :: proc(exprs: ^[dynamic]Expression) {
//}


