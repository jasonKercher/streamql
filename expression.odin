//+private
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
Expr_Asterisk :: distinct i8
Expr_Constant :: distinct Data
Expr_Grouping :: distinct ^Expression
Expr_Subquery :: distinct ^Query
Expr_Variable :: distinct i64
Expr_Null :: distinct string

Expr_Column_Name :: struct {
	item: Schema_Item,
	src_idx: i8,
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
	subq_idx: i8,
}

make_expression_const_i :: proc(val: i64) -> Expression {
	return Expression {
		data = Expr_Constant(val),
		data_type = .Int,
		subq_idx = -1,
	}
}

make_expression_const_f :: proc(val: f64) -> Expression {
	return Expression {
		data = Expr_Constant(val),
		data_type = .Float,
		subq_idx = -1,
	}
}

make_expression_const_s :: proc(s: string) -> Expression {
	/* Replace escaped ' in constant string */
	cpy, was_allocation := strings.replace_all(s, "''", "'")
	if !was_allocation {
		cpy = strings.clone(cpy)
	}
	return Expression {
		data = Expr_Constant(cpy),
		data_type = .String,
		subq_idx = -1,
	}
}

make_expression_subquery :: proc(subquery: ^Query) -> Expression {
	return Expression {
		data = Expr_Subquery(subquery),
		subq_idx = -1,
	}
}

make_expression_name :: proc(name, table_name: string) -> Expression {
	expr : Expression = {
		alias = strings.clone(name),
		table_name = table_name,
		subq_idx = -1,
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
		expr.data_type = .String
		expr.data = Expr_Column_Name {
			item = Schema_Item {name = strings.clone(name), loc = -1 },
			src_idx = -1,
		}
	}
	return expr
}

make_expression_agg :: proc(agg: Expr_Aggregate) -> Expression {
	return Expression {
		data = agg,
		subq_idx = -1,
	}
}

make_expression_fn :: proc(fn: Expr_Function) -> Expression {
	return Expression {
		data = fn,
		data_type = fn.data_type,
		subq_idx = -1,
	}
}

make_expression_null :: proc(null: Expr_Null) -> Expression {
	return Expression {
		data = null,
		subq_idx = -1,
	}
}

make_expression_case :: proc(c: Expr_Case) -> Expression {
	return Expression {
		data = c,
		subq_idx = -1,
	}
}

make_expression_asterisk :: proc(aster: Expr_Asterisk) -> Expression {
	return Expression {
		data = aster,
		subq_idx = -1,
	}
}

make_expression_var :: proc(var: Expr_Variable) -> Expression {
	return Expression {
		data = var,
		subq_idx = -1,
	}
}

make_expression_ref :: proc(expr: Expr_Reference) -> Expression {
	return Expression {
		data = expr,
		subq_idx = -1,
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

expression_link :: proc(col: ^Expr_Column_Name, item: Schema_Item, src_idx: int, src: ^Source) {
	col.item.loc = item.loc
	col.item.width = item.width
	col.src_idx = i8(src_idx)

	/* TODO: Aggregate linked expression */

	r := &src.schema.data.(Reader)
	if src != nil && item.loc > r.max_field_idx {
		r.max_field_idx = item.loc
	}
}

expression_get_int :: proc(expr: ^Expression, recs: ^Record = nil) -> (ret_val: i64, res: Result) {
	#partial switch v in &expr.data {
	case Expr_Constant:
		return data_to_int((^Data)(&v))
	case Expr_Column_Name:
		rec: ^Record
		if expr.subq_idx == -1 {
			rec = record_get(recs, v.src_idx)
		} else {
			rec = record_get(recs, expr.subq_idx)
		}

		if rec == nil || len(rec.fields) <= int(v.item.loc) {
			return 0, .Null
		}
		new_data: Data = rec.fields[v.item.loc]
		return data_to_int(&new_data)
	case Expr_Function:
		strings.reset_builder(&expr.buf)
		new_data := v.call__(&v, recs, &expr.buf) or_return
		return data_to_int(&new_data)
	}
	return 0, not_implemented()
}

expression_get_float :: proc(expr: ^Expression, recs: ^Record = nil) -> (ret_val: f64, res: Result) {
	#partial switch v in &expr.data {
	case Expr_Constant:
		return data_to_float((^Data)(&v))
	case Expr_Column_Name:
		rec: ^Record
		if expr.subq_idx == -1 {
			rec = record_get(recs, v.src_idx)
		} else {
			rec = record_get(recs, expr.subq_idx)
		}

		if rec == nil || len(rec.fields) <= int(v.item.loc) {
			return 0, .Null
		}
		new_data: Data = rec.fields[v.item.loc]
		return data_to_float(&new_data)
	case Expr_Function:
		strings.reset_builder(&expr.buf)
		new_data := v.call__(&v, recs, &expr.buf) or_return
		return data_to_float(&new_data)
	}
	return 0, not_implemented()
}

expression_get_string :: proc(expr: ^Expression, recs: ^Record = nil) -> (ret_val: string, res: Result) {
	#partial switch v in &expr.data {
	case Expr_Constant:
		return data_to_string((^Data)(&v), &expr.buf)
	case Expr_Column_Name:
		rec: ^Record
		if expr.subq_idx == -1 {
			rec = record_get(recs, v.src_idx)
		} else {
			rec = record_get(recs, expr.subq_idx)
		}

		if rec == nil || len(rec.fields) <= int(v.item.loc) {
			return "", .Null
		}
		return rec.fields[v.item.loc], .Ok
	case Expr_Function:
		strings.reset_builder(&expr.buf)
		new_data := v.call__(&v, recs, &expr.buf) or_return
		return data_to_string(&new_data, &expr.buf)
	}
	return "", not_implemented()
}

//expression_update_indicies :: proc(exprs: ^[dynamic]Expression) {
//}


