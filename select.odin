package streamql

import "core:strings"

Select :: struct {
	expressions: [dynamic]Expression,
}

make_select :: proc() -> Select {
	return Select {
		expressions = make([dynamic]Expression),
	}
}

select_add_expression :: proc(s: ^Select, expr: ^Expression) {
	append(&s.expressions, expr^)
}

select_apply_alias :: proc(s: ^Select, alias: string) {
	expr := &s.expressions[len(s.expressions) - 1]
	expr.alias = strings.clone(alias)
}
