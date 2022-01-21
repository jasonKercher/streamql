package streamql

import "core:fmt"
import "core:os"

Update :: struct {
	schema: Schema,
	writer: Writer,
	columns: [dynamic]Expression,
	values: [dynamic]Expression,
	top_count: i64,
}

update_add_expression :: proc(u: ^Update, expr: ^Expression) -> (^Expression, Result) {
	if len(u.values) == len(u.columns) {
		_, ok_col := expr.data.(Expr_Column_Name)
		_, ok_var := expr.data.(Expr_Variable)
		if !ok_col && !ok_var {
			fmt.fprintf(os.stderr, "unexpected expression as update element\n")
			return nil, .Error
		}
		append(&u.columns, expr^)
		return &u.columns[len(u.columns) - 1], .Ok
	}
	append(&u.values, expr^)
	return &u.values[len(u.values) - 1], .Ok
}

update_apply_process :: proc(q: ^Query) -> Result {
	return not_implemented()
}
