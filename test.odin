package streamql

import "core:intrinsics"
import "core:testing"
import "core:fmt"

EPSILON :: f64(.0002)
_sql: Streamql

@private
_init_sql :: proc() {
	cfg: bit_set[Config] = {} // lol
	construct(&_sql, cfg)
}

_check_val_float :: proc(t: ^testing.T, actual: f64, expected: f64, loc := #caller_location) {
	diff := abs(actual - expected)
	msg := fmt.tprintf("abs(%0.5f - %0.5f) = %0.5f", actual, expected, diff)
	testing.expect(t, diff < EPSILON, msg, loc)
}

_check_val_other :: proc(t: ^testing.T, actual: $T, expected: T, loc := #caller_location)
    where !intrinsics.type_is_float(T) {
	testing.expect_value(t, actual, expected, loc)
}

_check_value :: proc {_check_val_float, _check_val_other}

@private
_check_constant :: proc(t: ^testing.T, query: string, expected: $T, loc := #caller_location) {
	res: Result
	fields: []Field

	res = generate_plans(&_sql, query)
	testing.expect_value(t, res, Result.Ok, loc)

	fields, res = step(&_sql)
	testing.expect_value(t, res, Result.Running, loc)
	testing.expect_value(t, len(fields), 1, loc)

	v, ok := fields[0].data.(T)
	testing.expect(t, ok, "type mismatch", loc)
	_check_value(t, v, expected, loc)

	fields, res = step(&_sql)
	testing.expect_value(t, res, Result.Complete, loc)
}

@test
check_const_literals :: proc(t: ^testing.T) {
	_init_sql()
	_check_constant(t, "select 1", i64(1))
	_check_constant(t, "select 1.1", f64(1.1))
	_check_constant(t, "select 'x''y'", "x'y")
	_check_constant(t, "select 3/2", i64(1))
	_check_constant(t, "select 1.1 + 1", f64(2.1))
	_check_constant(t, "select 1 * 2.0 / (3 + 4.0)", f64(0.285714))
	_check_constant(t, "select 123 * -2.0 / (-3 + 4.1)", f64(-223.636263))
	_check_constant(t, "select '13' / 5", i64(2))
	_check_constant(t, "select '1' / 3.0", f64(.333333))
	_check_constant(t, "select '4' + '5'", "45")
	_check_constant(t, "select '1' + '9' + 5", i64(24))
	_check_constant(t, "select -9223372036854775806 + -2", i64(-9223372036854775808))
	_check_constant(t, "select -1", i64(-1))
	_check_constant(t, "select +1.1", f64(1.1))
	_check_constant(t, "select -(1 * 2)", i64(-2))
	_check_constant(t, "select ~1", i64(-2))

	_check_constant(t, "select left('testing sql', 4)", "test")

	destroy(&_sql)
}

