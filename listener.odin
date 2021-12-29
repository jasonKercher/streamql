package streamql

import "core:os"
import "core:fmt"

Listener_Status :: enum {
	Leaving_Block,
}

Parse_Mode :: enum {
	If,
	In,
	Set,
	Top,
	Case,
	Logic,
	Declare,
	Groupby,
	Orderby,
	Aggregate,
	Select_List,
	Update_List,
}

Logic_Mode :: enum {
	Case,
	Join,
	Where,
	Branch,
	Having,
}

Listener :: struct {
	query_stack: [dynamic]^Query,
	logic_stack: [dynamic]^Logic_Group,
	function_stack: [dynamic]Expression,
	status: bit_set[Listener_Status],
	sub_id: i16,
	mode: Query_Mode,
	logic_mode: Logic_Mode,
}

parse_enter_sql :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER SQL\n")
		return .Ok
	}

	l := &sql.listener
	l.sub_id = 0

	append(&l.query_stack, i32(len(sql.queries)))

	q := new_query(l.sub_id)
	q.idx = len(sql.queries)
	q.next_idx = q.idx + 1
	append(&sql.queries, q)

	return .Ok
}

parse_leave_sql :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE SQL\n")
		return .Ok
	}

	l := &sql.listener
	if .Leaving_Block in l.status {
		l.status -= {.Leaving_Block}
		return .Ok
	}

	prev_query := pop(&l.query_stack)
	prev_query.query_total = l.sub_id + 1

	prev_branch : ^Branch

	curr_query := _get_curr_query(sql)
	if prev_query.operation.(Branch) {
		prev_branch = &prev_query.operation.(Branch)
		switch prev_query.operation.(Branch).type {
		case .If:
			fallthrough
		case .Elseif:
			prev_branch.last_true_block_query.next_idx = len(sql.queries)
		case .While:
			prev_branch.last_true_block_query.next_idx = prev_query
		}
	}

	if prev_query == nil {
		sql.branch_state = .No_Branch
		return .Ok
	}

	curr_branch := &curr_query.operation.(Branch)

	#partial switch sql.branch_state {
	case .Expect_Expr:
		if prev_branch == nil || prev_branch.type == .If {
			curr_branch.last_true_block_query = prev_query
		}

		s := sql_get_curr_scope(sql)
		if .Is_In_Block in s.status {
			return .Ok
		}
		if .Expecting_Else in curr_branch.status {
			sql.branch_state = .Expect_Else
		} else {
			sql.branch_state = .Expect_Exit
			curr_branch.false_idx = len(sql.queries)
		}
	}

	return .Ok
}

parse_send_int :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND INT %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_send_float :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND FLOAT %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_send_string :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND STRING %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_send_name :: proc(sql: ^Streamql, tok: ^Token, table_name_tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
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
	return .Ok
}

parse_send_asterisk :: proc(sql: ^Streamql, tok: ^Token, table_name_tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		if (table_name_tok == nil) {
			fmt.fprintf(os.stderr, "SEND ASTERISK\n")
			return .Ok
		}
		fmt.fprintf(os.stderr,
		        "SEND ASTERISK (WITH TABLE %s)\n",
		        token_to_string(&sql.parser, table_name_tok))
		return .Ok
	}
	return .Ok
}

parse_send_variable :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND VAR %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_send_column_alias :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND COLUMN ALIAS %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_enter_subquery_const :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER SUBQUERY CONST\n")
		return .Ok
	}
	return .Ok
}

parse_leave_subquery_const :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE SUBQUERY CONST\n")
		return .Ok
	}
	return .Ok
}

parse_enter_function :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		#partial switch tok.type {
		case .Sym_Minus_Unary:
		case .Sym_Plus_Unary:
		case .Sym_Bit_Not_Unary:
			fmt.fprintf(os.stderr, "ENTER UNARY %s\n", token_to_string(&sql.parser, tok))
		}
		fmt.fprintf(os.stderr, "ENTER FUNCTION %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_leave_function :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		#partial switch tok.type {
		case .Sym_Minus_Unary:
		case .Sym_Plus_Unary:
		case .Sym_Bit_Not_Unary:
			fmt.fprintf(os.stderr, "LEAVE UNARY %s\n", token_to_string(&sql.parser, tok))
		}
		fmt.fprintf(os.stderr, "LEAVE FUNCTION %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_send_select_stmt :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND SELECT STMT\n")
		return .Ok
	}

	q := _get_curr_query(sql)
	q.mode = .Select_List
	q.operation = make_select()
	_check_for_else(sql)

	return .Ok
}

parse_send_into_name :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND INTO NAME %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_enter_from :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER FROM\n")
		return .Ok
	}
	return .Ok
}

parse_leave_from :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE FROM\n")
		return .Ok
	}
	return .Ok
}

parse_send_table_source :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND TABLE SOURCE %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_enter_subquery_source :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER SUBQUERY SOURCE\n")
		return .Ok
	}
	return .Ok
}

parse_leave_subquery_source :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE SUBQUERY SOURCE\n")
		return .Ok
	}
	return .Ok
}

parse_send_source_alias :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND SOURCE ALIAS %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	return .Ok
}

parse_send_join_type :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
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
	return .Ok
}

parse_enter_where :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER WHERE\n")
		return .Ok
	}
	return .Ok
}

parse_leave_where :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE WHERE\n")
		return .Ok
	}
	return .Ok
}

parse_enter_predicate :: proc(sql: ^Streamql, tok: ^Token, is_not_predicate, is_not: bool) -> Result {
	if .Parse_Only in sql.config {
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
	return .Ok
}

parse_leave_predicate :: proc(sql: ^Streamql, tok: ^Token, is_not_predicate, is_not: bool) -> Result {
	if .Parse_Only in sql.config {
		if is_not_predicate {
			if is_not {
				fmt.fprintf(os.stderr, "LEAVE PREDICATE NOT %s NOT\n", token_to_string(&sql.parser, tok))
			} else {
				fmt.fprintf(os.stderr, "LEAVE PREDICATE NOT %s\n", token_to_string(&sql.parser, tok))
			}
		} else {
			if is_not {
				fmt.fprintf(os.stderr, "LEAVE PREDICATE %s NOT\n", token_to_string(&sql.parser, tok))
			} else {
				fmt.fprintf(os.stderr, "LEAVE PREDICATE %s\n", token_to_string(&sql.parser, tok))
			}
		}
		return .Ok
	}
	return .Ok
}

parse_enter_and :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER AND\n")
		return .Ok
	}
	return .Ok
}

parse_leave_and :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE AND\n")
		return .Ok
	}
	return .Ok
}

parse_enter_or :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER OR\n")
		return .Ok
	}
	return .Ok
}

parse_leave_or :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE OR\n")
		return .Ok
	}
	return .Ok
}

parse_enter_not :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER NOT\n")
		return .Ok
	}
	return .Ok
}

parse_leave_not :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE NOT\n")
		return .Ok
	}
	return .Ok
}

parse_enter_groupby :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER GROUP BY\n")
		return .Ok
	}
	return .Ok
}

parse_leave_groupby :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE GROUP BY\n")
		return .Ok
	}
	return .Ok
}

parse_enter_having :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER HAVING\n")
		return .Ok
	}
	return .Ok
}

parse_leave_having :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE HAVING\n")
		return .Ok
	}
	return .Ok
}

parse_send_distinct :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND DISTINCT\n")
		return .Ok
	}
	return .Ok
}

parse_send_all :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND ALL\n")
		return .Ok
	}
	return .Ok
}

parse_enter_top_expr :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER TOP EXPR\n")
		return .Ok
	}
	return .Ok
}

parse_leave_top_expr :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE TOP EXPR\n")
		return .Ok
	}
	return .Ok
}

@(private = "file")
_check_for_else :: proc(sql: ^Streamql) {
	if fql.branch_state != .Expect_Else {
		return
	}

	q := _get_curr_query(sql)

	l := &sql.listener
	branch_query := query_stack[len(l.query_stack) - 2]

	branch := &branch_query.operation.(Branch)

	op, ok := q.operation.(branch)

	/* Naked else */
	if !ok || op.type  == .While {
		branch.else_scope = i32(len(sql.scopes))
		append(&sql.scopes, make_scope())
		
		s := &sql.scopes[len(sql.scopes) - 1]

		s.parent_scope = branch.scope.parent_scope
		sql.curr_scope = branch.else_scope
		return
	}

	/* If we made it this far, we have entered an "else if" */
	sql.branch_state = .Expect_Expr
	else_if_stmt := &sql.queries[len(sql.queries) -1]
	else_if := elseif_stmt.operation.(Branch).type = .Else_If
}

@(private = "file")
_get_curr_query :: proc(sql: ^Streamql) -> ^Query {
	if len(sql.query_stack) == 0 {
		return nil
	}
	return sql.query_stack[len(sql.query_stack) - 1]
}
