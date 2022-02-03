//+private
package streamql


sql_read :: proc(process: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_cartesian_join :: proc(process: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_hash_join :: proc(process: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_left_join_logic :: proc(process: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_logic :: proc(process: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_groupby :: proc(process: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_select :: proc(process: ^Process) -> Process_Result {
	main_select := process.data.(^Select)
	current_select := &main_select.select_list[main_select.select_idx]



	return .Error
}
