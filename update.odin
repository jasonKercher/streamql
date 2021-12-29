package streamql

import "core:fmt"
import "core:os"

Update :: struct {
	columns: [dynamic]Expression,
	values: [dynamic]Expression,
}

update_add_expression :: proc(u: ^Update, expr: ^Expression) -> Result {
	if len(u.values) == len(u.columns) {
		if expr.type != .Column_Name && expr.type != .Variable {
			fmt.fprintf(os.stderr, "unexpected expression as update element\n")
			return .Error
		}
		append(&u.columns, expr^)
		return .Ok
	}
	append(&u.values, expr^)
	return .Ok
}
