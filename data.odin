//+private
package streamql

import "core:strconv"
import "core:strings"
import "core:fmt"

DATA_TYPE_COUNT :: 3

data_determine_type :: proc(t0, t1: Data_Type) -> Data_Type {
	if t0 == t1 {
		return t0
	}
	if t0 == .String {
		return t1
	}
	if t1 == .String {
		return t0
	}
	return .Float
}

data_to_int :: proc(data: ^Data) -> (i64, Result) {
	switch v in data {
	case i64:
		return v, .Ok
	case f64:
		return i64(v), .Ok
	case string:
		new_val, ok := strconv.parse_i64(v)
		if !ok {
			fmt.eprintf("failed to convert `%s' to an integer\n", v)
			return 0, .Error
		}
		return new_val, .Ok
	}
	unreachable()
}

data_to_float :: proc(data: ^Data) -> (f64, Result) {
	switch v in data {
	case i64:
		return f64(v), .Ok
	case f64:
		return v, .Ok
	case string:
		new_val, ok := strconv.parse_f64(v)
		if !ok {
			fmt.eprintf("failed to convert `%s' to a float\n", v)
			return 0, .Error
		}
		return new_val, .Ok
	}
	unreachable()
}

data_to_string :: proc(data: ^Data, sb: ^strings.Builder = nil) -> (string, Result) {
	switch v in data {
	case i64:
		if sb == nil {
			return "", .Error
		}
		strings.reset_builder(sb)
		return fmt.sbprintf(sb, "%i", v), .Ok
	case f64:
		if sb == nil {
			return "", .Error
		}
		strings.reset_builder(sb)
		return fmt.sbprintf(sb, "%f", v), .Ok
	case string:
		return v, .Ok
	}
	unreachable()
}
