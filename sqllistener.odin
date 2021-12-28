package streamql

import "core:os"
import "core:fmt"

parse_send_int :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND INT %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_send_float :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND FLOAT %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_send_string :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND STRING %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_send_name :: proc(sql: ^Streamql, tok: ^Token, table_name_tok: ^Token) -> Sql_Result {
	if (table_name_tok == nil) {
		fmt.fprintf(os.stderr, "SEND NAME %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	fmt.fprintf(os.stderr,
	        "SEND NAME %s (WITH TABLE %s)\n",
	        token_to_string(&sql.parser, tok),
	        token_to_string(&sql.parser, table_name_tok))
	return .Ok
}

parse_send_asterisk :: proc(sql: ^Streamql, tok: ^Token, table_name_tok: ^Token) -> Sql_Result {
	if (table_name_tok == nil) {
		fmt.fprintf(os.stderr, "SEND ASTERISK\n")
		return .Ok
	}
	fmt.fprintf(os.stderr,
	        "SEND ASTERISK (WITH TABLE %s)\n",
	        token_to_string(&sql.parser, table_name_tok))
	return .Ok
}

parse_send_variable :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND VAR %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_send_column_alias :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND COLUMN ALIAS %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_enter_subquery_const :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER SUBQUERY CONST\n")
	return .Ok
}

parse_leave_subquery_const :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE SUBQUERY CONST\n")
	return .Ok
}

parse_enter_function :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	#partial switch tok.type {
	case .Sym_Minus_Unary:
	case .Sym_Plus_Unary:
	case .Sym_Bit_Not_Unary:
		fmt.fprintf(os.stderr, "ENTER UNARY %s\n", token_to_string(&sql.parser, tok))
	}
	fmt.fprintf(os.stderr, "ENTER FUNCTION %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_leave_function :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	#partial switch tok.type {
	case .Sym_Minus_Unary:
	case .Sym_Plus_Unary:
	case .Sym_Bit_Not_Unary:
		fmt.fprintf(os.stderr, "LEAVE UNARY %s\n", token_to_string(&sql.parser, tok))
	}
	fmt.fprintf(os.stderr, "LEAVE FUNCTION %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_send_select_stmt :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND SELECT STMT\n")
	return .Ok
}

//int parse_enter_select_stmt(sql: ^Streamql)
//{
//	fmt.fprintf(os.stderr, "ENTER SELECT STMT\n")
//}
//
//int parse_leave_select_stmt(sql: ^Streamql)
//{
//	fmt.fprintf(os.stderr, "LEAVE SELECT STMT\n")
//}

parse_send_into_name :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND INTO NAME %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_enter_from :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER FROM\n")
	return .Ok
}

parse_leave_from :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE FROM\n")
	return .Ok
}

parse_send_table_source :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND TABLE SOURCE %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_enter_subquery_source :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER SUBQUERY SOURCE\n")
	return .Ok
}

parse_leave_subquery_source :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE SUBQUERY SOURCE\n")
	return .Ok
}

parse_send_source_alias :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND SOURCE ALIAS %s\n", token_to_string(&sql.parser, tok))
	return .Ok
}

parse_send_join_type :: proc(sql: ^Streamql, tok: ^Token) -> Sql_Result {
	type_str: string

	#partial switch tok.type {
	case .Inner:
		type_str = "INNER"
		break
	case .Left:
		type_str = "LEFT"
		break
	case .Right:
		type_str = "RIGHT"
		break
	case .Full:
		type_str = "FULL"
		break
	case .Cross:
		type_str = "CROSS"
		break
	case:
		fmt.fprintf(os.stderr, "UNKNOWN JOIN TYPE")
		/* Return Error? This shouldn't really be possible */
	}
	fmt.fprintf(os.stderr, "SEND JOIN TYPE %s\n", type_str)
	return .Ok
}

parse_enter_where :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER WHERE\n")
	return .Ok
}

parse_leave_where :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE WHERE\n")
	return .Ok
}

parse_enter_predicate :: proc(sql: ^Streamql, tok: ^Token, is_not_predicate, is_not: bool) -> Sql_Result {
	if is_not_predicate {
		if is_not {
			fmt.fprintf(os.stderr, "ENTER PREDICATE NOT %s NOT\n", token_to_string(&sql.parser, tok))
		} else {
			fmt.fprintf(os.stderr, "ENTER PREDICATE NOT %s\n", token_to_string(&sql.parser, tok))
		}
	} else {
		if is_not {
			fmt.fprintf(os.stderr, "ENTER PREDICATE %s NOT\n", token_to_string(&sql.parser, tok))
		} else {
			fmt.fprintf(os.stderr, "ENTER PREDICATE %s\n", token_to_string(&sql.parser, tok))
		}
	}
	return .Ok
}

parse_leave_predicate :: proc(sql: ^Streamql, tok: ^Token, is_not_predicate, is_not: bool) -> Sql_Result {
	if is_not_predicate {
		if is_not {
			fmt.fprintf(os.stderr, "ENTER PREDICATE NOT %s NOT\n", token_to_string(&sql.parser, tok))
		} else {
			fmt.fprintf(os.stderr, "ENTER PREDICATE NOT %s\n", token_to_string(&sql.parser, tok))
		}
	} else {
		if is_not {
			fmt.fprintf(os.stderr, "ENTER PREDICATE %s NOT\n", token_to_string(&sql.parser, tok))
		} else {
			fmt.fprintf(os.stderr, "ENTER PREDICATE %s\n", token_to_string(&sql.parser, tok))
		}
	}
	return .Ok
}

parse_enter_and :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER AND\n")
	return .Ok
}

parse_leave_and :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE AND\n")
	return .Ok
}

parse_enter_or :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER OR\n")
	return .Ok
}

parse_leave_or :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE OR\n")
	return .Ok
}

parse_enter_not :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER NOT\n")
	return .Ok
}

parse_leave_not :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE NOT\n")
	return .Ok
}

parse_enter_groupby :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER GROUP BY\n")
	return .Ok
}

parse_leave_groupby :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE GROUP BY\n")
	return .Ok
}

parse_enter_having :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER HAVING\n")
	return .Ok
}

parse_leave_having :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE HAVING\n")
	return .Ok
}

parse_send_distinct :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND DISTINCT\n")
	return .Ok
}

parse_send_all :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "SEND ALL\n")
	return .Ok
}

parse_enter_top_expr :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "ENTER TOP EXPR\n")
	return .Ok
}

parse_leave_top_expr :: proc(sql: ^Streamql) -> Sql_Result {
	fmt.fprintf(os.stderr, "LEAVE TOP EXPR\n")
	return .Ok
}
