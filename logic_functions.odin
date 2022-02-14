package streamql

import "util"

@(private = "file")
_get_ints :: proc(l: ^Logic, recs: ^Record) -> (n0: i64, n1: i64, res: Result) {
	n0 = expression_get_int(&l.exprs[0], recs) or_return
	n1 = expression_get_int(&l.exprs[1], recs) or_return
	return n0, n1, .Ok
}

@(private = "file")
_get_floats :: proc(l: ^Logic, recs: ^Record) -> (n0: f64, n1: f64, res: Result) {
	n0 = expression_get_float(&l.exprs[0], recs) or_return
	n1 = expression_get_float(&l.exprs[1], recs) or_return
	return n0, n1, .Ok
}

@(private = "file")
_get_strings :: proc(l: ^Logic, recs: ^Record) -> (s0: string, s1: string, res: Result) {
	s0 = expression_get_string(&l.exprs[0], recs) or_return
	s1 = expression_get_string(&l.exprs[1], recs) or_return
	return s0, s1, .Ok
}


sql_logic_eq_i :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_ints(l, recs) or_return
	return n0 == n1, .Ok
}

/* Maybe use an epsilon?? */
sql_logic_eq_f :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_floats(l, recs) or_return
	return n0 == n1, .Ok
}

sql_logic_eq_s :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	s0, s1 := _get_strings(l, recs) or_return
	return util.string_compare_nocase_rtrim(s0, s1) == 0, .Ok
}

sql_logic_ne_i :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_ints(l, recs) or_return
	return n0 != n1, .Ok
}

sql_logic_ne_f :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_floats(l, recs) or_return
	return n0 != n1, .Ok
}

sql_logic_ne_s :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	s0, s1 := _get_strings(l, recs) or_return
	return util.string_compare_nocase_rtrim(s0, s1) != 0, .Ok
}

sql_logic_gt_i :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_ints(l, recs) or_return
	return n0 > n1, .Ok
}

sql_logic_gt_f :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_floats(l, recs) or_return
	return n0 > n1, .Ok
}

sql_logic_gt_s :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	s0, s1 := _get_strings(l, recs) or_return
	return util.string_compare_nocase_rtrim(s0, s1) > 0, .Ok
}

sql_logic_ge_i :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_ints(l, recs) or_return
	return n0 >= n1, .Ok
}

sql_logic_ge_f :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_floats(l, recs) or_return
	return n0 >= n1, .Ok
}

sql_logic_ge_s :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	s0, s1 := _get_strings(l, recs) or_return
	return util.string_compare_nocase_rtrim(s0, s1) >= 0, .Ok
}

sql_logic_lt_i :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_ints(l, recs) or_return
	return n0 < n1, .Ok
}

sql_logic_lt_f :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_floats(l, recs) or_return
	return n0 < n1, .Ok
}

sql_logic_lt_s :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	s0, s1 := _get_strings(l, recs) or_return
	return util.string_compare_nocase_rtrim(s0, s1) < 0, .Ok
}

sql_logic_le_i :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_ints(l, recs) or_return
	return n0 <= n1, .Ok
}

sql_logic_le_f :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	n0, n1 := _get_floats(l, recs) or_return
	return n0 <= n1, .Ok
}

sql_logic_le_s :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	s0, s1 := _get_strings(l, recs) or_return
	return util.string_compare_nocase_rtrim(s0, s1) <= 0, .Ok
}

sql_logic_in_i :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	return false, not_implemented()
}

sql_logic_in_f :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	return false, not_implemented()
}

sql_logic_in_s :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	return false, not_implemented()
}

sql_logic_subin_i :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	return false, not_implemented()
}

sql_logic_subin_f :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	return false, not_implemented()
}

sql_logic_subin_s :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	return false, not_implemented()
}

sql_logic_is_null :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	return false, not_implemented()
}

sql_logic_like :: proc(l: ^Logic, recs: ^Record) -> (truthy: bool, res: Result) {
	return false, not_implemented()
}



