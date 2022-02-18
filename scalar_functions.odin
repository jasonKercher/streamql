//+private
package streamql

import "core:fmt"
import "core:strings"
import "core:unicode/utf8"

/** operations **/

overflow_safe_add_i :: proc(n0, n1: i64) -> (i64, Result) {
	if (n0 > 0 && n1 > 0 && n0 + n1 < 0) || (n0 < 0 && n1 < 0 && n0 + n1 >= 0) {
		fmt.eprintf("Arithmetic overflow `%d + %d'\n", n0, n1)
		return 0, .Error
	}
	return n0 + n1, .Ok
}

overflow_safe_minus_i :: proc(n0, n1: i64) -> (i64, Result) {
	if (n0 > 0 && n1 < 0 && n0 - n1 < 0) || (n0 < 0 && n1 > 0 && n0 - n1 >= 0) {
		fmt.eprintf("Arithmetic overflow `%d - %d'\n", n0, n1)
		return 0, .Error
	}
	return n0 - n1, .Ok
}

overflow_safe_mult_i :: proc(n0, n1: i64) -> (i64, Result) {
	if n0 == 0 || n1 == 0 {
		return 0, .Ok
	}

	if (n0 * n1) / n0 != n1 {
		fmt.eprintf("Arithmetic overflow `%d * %d'\n", n0, n1)
		return 0, .Error
	}

	return n0 * n1, .Ok
}


sql_op_plus_i :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	return overflow_safe_add_i(n0, n1)
}

sql_op_plus_f :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_float(&fn.args[0], recs) or_return
	n1 := expression_get_float(&fn.args[1], recs) or_return
	return n0 + n1, .Ok
}

sql_op_plus_s :: proc(fn: ^Expr_Function, recs: ^Record = nil, sb: ^strings.Builder) -> (val: Data, res: Result) {
	s0 := expression_get_string(&fn.args[0], recs) or_return
	s1 := expression_get_string(&fn.args[1], recs) or_return
	strings.write_string(sb, s0)
	strings.write_string(sb, s1)
	return strings.to_string(sb^), .Ok
}

sql_op_minus_i :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	return overflow_safe_minus_i(n0, n1)
}

sql_op_minus_f :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_float(&fn.args[0], recs) or_return
	n1 := expression_get_float(&fn.args[1], recs) or_return
	return n0 - n1, .Ok
}

sql_op_mult_i :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	return overflow_safe_mult_i(n0, n1)
}

sql_op_mult_f :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_float(&fn.args[0], recs) or_return
	n1 := expression_get_float(&fn.args[1], recs) or_return
	return n0 * n1, .Ok
}

sql_op_divi_i :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	return n0 / n1, .Ok
}

sql_op_divi_f :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_float(&fn.args[0], recs) or_return
	n1 := expression_get_float(&fn.args[1], recs) or_return
	return n0 / n1, .Ok
}

sql_op_mod_i :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	return n0 % n1, .Ok
}

sql_op_bit_or :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	return n0 | n1, .Ok
}

sql_op_bit_and :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	return n0 & n1, .Ok
}

sql_op_bit_xor :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	return n0 ~ n1, .Ok
}

sql_op_bit_not :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n := expression_get_int(&fn.args[0], recs) or_return
	return ~n, .Ok
}

sql_op_unary_minus_i :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n := expression_get_int(&fn.args[0], recs) or_return
	if n == min(type_of(n)) {
		fmt.eprintf("Arithmetic overflow `-(%d)'\n", n)
		return 0, .Error
	}
	return -n, .Ok
}

sql_op_unary_minus_f :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	n := expression_get_float(&fn.args[0], recs) or_return
	return -n, .Ok
}

sql_op_unary_plus_i :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	return expression_get_int(&fn.args[0], recs)
}

sql_op_unary_plus_f :: proc(fn: ^Expr_Function, recs: ^Record = nil, _: ^strings.Builder = nil) -> (val: Data, res: Result) {
	return expression_get_float(&fn.args[0], recs)
}

/** Named scalar functions **/


sql_left :: proc(fn: ^Expr_Function, recs: ^Record = nil, sb: ^strings.Builder = nil) -> (val: Data, res: Result) {
	s := expression_get_string(&fn.args[0], recs) or_return
	n := _get_length(&fn.args[1], recs) or_return
	
	i: int
	r: rune
	rune_len: int
	rune_qty: int
	for ; i < len(s) && rune_qty < n; i += rune_len {
		rune_qty += 1
		r, rune_len = utf8.decode_rune_in_string(s[i:])
		if r == utf8.RUNE_ERROR {
			fmt.eprintf("invalid UTF-8 sequence `%s'\n", s)
			return "", .Error
		}
	}
	strings.write_string(sb, s[:i])
	return strings.to_string(sb^), .Ok
}

sql_left_byte :: proc(fn: ^Expr_Function, recs: ^Record = nil, sb: ^strings.Builder = nil) -> (val: Data, res: Result) {
	s := expression_get_string(&fn.args[0], recs) or_return
	n := _get_length(&fn.args[1], recs) or_return

	limit := min(len(s), int(n))
	strings.write_string(sb, s[0:limit])
	return strings.to_string(sb^), .Ok
}


sql_right :: proc(fn: ^Expr_Function, recs: ^Record = nil, sb: ^strings.Builder = nil) -> (val: Data, res: Result) {
	s := expression_get_string(&fn.args[0], recs) or_return
	n := _get_length(&fn.args[1], recs) or_return
	
	i := len(s)
	r: rune
	rune_len: int
	rune_qty: int
	for ; i >= 0 && rune_qty < n; i -= rune_len {
		rune_qty += 1
		r, rune_len = utf8.decode_last_rune_in_string(s[:i])
		if r == utf8.RUNE_ERROR {
			fmt.eprintf("invalid UTF-8 sequence `%s'\n", s)
			return "", .Error
		}
	}
	strings.write_string(sb, s[i:])
	return strings.to_string(sb^), .Ok
}

sql_right_byte :: proc(fn: ^Expr_Function, recs: ^Record = nil, sb: ^strings.Builder = nil) -> (val: Data, res: Result) {
	s := expression_get_string(&fn.args[0], recs) or_return
	n := _get_length(&fn.args[1], recs) or_return

	limit := min(len(s), int(n))
	strings.write_string(sb, s[len(s) - limit:])
	return strings.to_string(sb^), .Ok
}

@(private = "file")
_get_length :: proc(expr: ^Expression, recs: ^Record) -> (length: int, res: Result) {
	n := expression_get_int(expr, recs) or_return
	if n < 0 {
		fmt.eprintf("invalid length value `%d'\n", n)
		return 0, .Error
	}
	return int(n), .Ok
}
