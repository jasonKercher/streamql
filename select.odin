package streamql

import "core:strings"

Select_Call :: proc(sel: ^Select) -> Result

Select :: struct {
	select__: Select_Call,
	writer: Writer,
	top_count: i64,
}

make_select :: proc() -> Select {
	return Select {
		top_count = 0,
	}
}
