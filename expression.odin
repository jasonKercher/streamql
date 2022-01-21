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
	Expr_Grouping,
	Expr_Subquery,
	Expr_Variable,
	Expr_Null,
}

Expression :: struct {
	buf: strings.Builder,
	alias: string,
	table_name: string,
	data: Expression_Data,
	props: bit_set[Expression_Props],
	subq_idx: u16,
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
	case:
		expr.data = Expr_Column_Name { name = strings.clone(name), col_idx = -1 }
	}
	return expr
}

make_expression_null :: proc(null: Expr_Null) -> Expression {
	return Expression {
		data = null,
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
	make_expression_subquery,
	make_expression_name,
	make_expression_null,
	make_expression_asterisk,
	make_expression_var,
	make_expression_ref,
}

destroy_expression :: proc(expr: ^Expression) {
	strings.destroy_builder(&expr.buf)
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
	case Expr_Subquery:
		strings.write_string(b, "<subquery>")
	case Expr_Variable:
		strings.write_string(b, "var<")
		strings.write_i64(b, i64(v))
		strings.write_byte(b, '>')
	case Expr_Null:
		strings.write_string(b, "NULL")
	}
}

expression_get_int :: proc(expr: ^Expression) -> (i64, Result) {
	return 0, .Ok
}

expression_get_float :: proc(expr: ^Expression) -> (f64, Result) {
	return 0, .Ok
}

expression_get_string :: proc(expr: ^Expression) -> (string, Result) {
	return "", .Ok
}

//expression_update_indicies :: proc(exprs: ^[dynamic]Expression) {
//}


