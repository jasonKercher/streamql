package streamql

import "core:os"
import "core:fmt"
import "core:strings"
import "core:strconv"

Listener_Status :: enum u8 {
	Leaving_Block,
}

Listener_Mode :: enum u8 {
	Undefined,
	If,
	In,
	Set,
	Top,
	Case,
	Declare,
	Groupby,
	Orderby,
	Aggregate,
	Select_List,
	Update_List,
}

Listener_State :: struct {
	f_stack: [dynamic]^Expression,
	mode: Listener_Mode,
}

Listener :: struct {
	state_stack: [dynamic]Listener_State,
	query_stack: [dynamic]^Query,
	status: bit_set[Listener_Status],
	sub_id: i16,
}
