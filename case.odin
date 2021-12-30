package streamql

Case_State :: enum u8 {
	Static,
	Static_Cmp,
	Logic_Group,
	Value,
}

Case :: struct {
	values: [dynamic]Expression,
	tests: [dynamic]^Logic_Group,
	state: Case_State,
	return_state: Listener_State,
}
