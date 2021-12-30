package streamql

import "core:os"
import "core:fmt"
import "core:strings"
import "core:strconv"

Listener_Status :: enum u8 {
	Leaving_Block,
}

Listener_Mode :: enum u8 {
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

Listener_Logic_Mode :: enum u8 {
	Case,
	Join,
	Where,
	Branch,
	Having,
}

Listener_State :: struct {
	f_stack: [dynamic]Expression,
	l_stack: [dynamic]^Logic_Group,
	mode: Listener_Mode,
	l_mode: Listener_Logic_Mode,
}

Listener :: struct {
	state: Listener_State,
	query_stack: [dynamic]^Query,
	status: bit_set[Listener_Status],
	sub_id: i16,
}

parse_enter_sql :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER SQL\n")
		return .Ok
	}

	l := &sql.listener
	l.sub_id = 0

	q := new_query(l.sub_id)
	q.idx = u32(len(sql.queries))
	q.next_idx = q.idx + 1
	append(&sql.queries, q)
	append(&l.query_stack, q) 

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
	if b, ok := prev_query.operation.(Branch); ok {
		prev_branch = &prev_query.operation.(Branch)
		switch prev_query.operation.(Branch).type {
		case .If:
			fallthrough
		case .Else_If:
			prev_branch.last_true_block_query.next_idx = u32(len(sql.queries))
		case .While:
			prev_branch.last_true_block_query.next_idx = prev_query.idx
		}
	}

	if curr_query == nil {
		sql.branch_state = .No_Branch
		return .Ok
	}

	curr_branch := &curr_query.operation.(Branch)

	#partial switch sql.branch_state {
	case .Expect_Expr:
		if prev_branch == nil || prev_branch.type == .If {
			curr_branch.last_true_block_query = prev_query
		}

		s := &sql.scopes[sql.curr_scope]
		if .Is_In_Block in s.status {
			return .Ok
		}
		if .Expecting_Else in curr_branch.status {
			sql.branch_state = .Expect_Else
		} else {
			sql.branch_state = .Expect_Exit
			curr_branch.false_idx = i32(len(sql.queries))
		}
	}

	return .Ok
}

parse_send_int :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	s := token_to_string(&sql.parser, tok)
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND INT %s\n", s)
		return .Ok
	}

	val: i64
	ok: bool
	if val, ok = strconv.parse_i64(s); !ok {
		fmt.fprintf(os.stderr, "Failed to convert `%s' to integer\n", s)
		return .Error
	}
	expr := make_expression(val)
	return query_distribute_expression(sql, _get_curr_query(sql), &expr)
}

parse_send_float :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	s := token_to_string(&sql.parser, tok)
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND FLOAT %s\n", s)
		return .Ok
	}

	val: f64
	ok: bool
	if val, ok = strconv.parse_f64(s); !ok {
		fmt.fprintf(os.stderr, "Failed to convert `%s' to float\n", s)
		return .Error
	}
	expr := make_expression(val)
	return query_distribute_expression(sql, _get_curr_query(sql), &expr)
}

parse_send_string :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	s := token_to_string(&sql.parser, tok)
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND STRING %s\n", s)
		return .Ok
	}
	expr := make_expression(s)
	return query_distribute_expression(sql, _get_curr_query(sql), &expr)
}

parse_send_name :: proc(sql: ^Streamql, tok: ^Token, table_name_tok: ^Token) -> Result {
	field_str := token_to_string(&sql.parser, tok)
	table_str := token_to_string(&sql.parser, table_name_tok)
	if .Parse_Only in sql.config {
		if (table_str == "") {
			fmt.fprintf(os.stderr, "SEND NAME %s\n", field_str)
			return .Ok
		}
		fmt.fprintf(os.stderr,
		        "SEND NAME %s (WITH TABLE %s)\n",
			field_str,
			table_str)
		return .Ok
	}
	expr := make_expression(field_str, table_str)
	return query_distribute_expression(sql, _get_curr_query(sql), &expr)
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
	expr := make_expression(Expression_Type.Asterisk)
	return query_distribute_expression(sql, _get_curr_query(sql), &expr)
}

parse_send_variable :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND VAR %s\n", token_to_string(&sql.parser, tok))
		return .Ok
	}
	expr := make_expression(Expression_Type.Variable)
	return query_distribute_expression(sql, _get_curr_query(sql), &expr)
}

parse_send_column_alias :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	s := token_to_string(&sql.parser, tok)
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND COLUMN ALIAS %s\n", s)
		return .Ok
	}
	q := _get_curr_query(sql)
	select_apply_alias(&q.operation.(Select), s)
	return .Ok
}

parse_enter_subquery_const :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER SUBQUERY CONST\n")
		return .Ok
	}

	l := &sql.listener
	l.sub_id += 1

	subquery := new_query(l.sub_id)
	append(&l.query_stack, subquery)

	return .Ok
}

parse_leave_subquery_const :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE SUBQUERY CONST\n")
		return .Ok
	}

	l := &sql.listener
	subquery := pop(&l.query_stack)
	q := _get_curr_query(sql)

	expr := make_expression(subquery)
	return query_distribute_expression(sql, q, &expr)
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
	sql.listener.state.mode = .Select_List
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

//parse_enter_from :: proc(sql: ^Streamql) -> Result {
//	if .Parse_Only in sql.config {
//		fmt.fprintf(os.stderr, "ENTER FROM\n")
//		return .Ok
//	}
//	return .Ok
//}
//parse_leave_from :: proc(sql: ^Streamql) -> Result {
//	if .Parse_Only in sql.config {
//		fmt.fprintf(os.stderr, "LEAVE FROM\n")
//		return .Ok
//	}
//	return .Ok
//}

parse_enter_subquery_source :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "ENTER SUBQUERY SOURCE\n")
		return .Ok
	}

	l := &sql.listener
	l.sub_id += 1

	subquery := new_query(l.sub_id)
	append(&l.query_stack, subquery)

	return .Ok
}

parse_leave_subquery_source :: proc(sql: ^Streamql) -> Result {
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "LEAVE SUBQUERY SOURCE\n")
		return .Ok
	}

	l := &sql.listener
	subquery := pop(&l.query_stack)
	
	q := _get_curr_query(sql)

	return query_add_subquery_source(q, subquery)
}

parse_send_table_source :: proc(sql: ^Streamql, chain: []^Token) -> Result {
	table_name := token_to_string(&sql.parser, chain[len(chain) - 1])
	schema_name := ""
	database_name := ""  /* probably never used... */
	server_name := ""    /* probably never used... */
	switch len(chain) {
	case 4:
		schema_name = token_to_string(&sql.parser, chain[2])
		database_name = token_to_string(&sql.parser, chain[1])
		server_name = token_to_string(&sql.parser, chain[0])
	case 3:
		schema_name = token_to_string(&sql.parser, chain[1])
		database_name = token_to_string(&sql.parser, chain[0])
	case 2:
		schema_name = token_to_string(&sql.parser, chain[1])
	}

	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND TABLE SOURCE %s\n", table_name)
		return .Ok
	}

	q := _get_curr_query(sql)
	return query_add_source(sql, q, table_name, schema_name)
}

parse_send_source_alias :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	s := token_to_string(&sql.parser, tok)
	if .Parse_Only in sql.config {
		fmt.fprintf(os.stderr, "SEND SOURCE ALIAS %s\n", s)
		return .Ok
	}
	q := _get_curr_query(sql)
	src := &q.sources[len(q.sources) - 1]
	src.alias = strings.clone(s)
	return .Ok
}

parse_send_join_type :: proc(sql: ^Streamql, tok: ^Token) -> Result {
	if .Parse_Only in sql.config {
		type_str: string

		#partial switch tok.type {
		case .Inner:
			type_str = "INNER"
		case .Left:
			type_str = "LEFT"
		case .Right:
			type_str = "RIGHT"
		case .Full:
			type_str = "FULL"
		case .Cross:
			type_str = "CROSS"
		case:
			fmt.fprintf(os.stderr, "UNKNOWN JOIN TYPE")
			/* Return Error? This shouldn't really be possible */
		}
		fmt.fprintf(os.stderr, "SEND JOIN TYPE %s\n", type_str)
		return .Ok
	}

	q := _get_curr_query(sql)
	src := &q.sources[len(q.sources) - 1]
	#partial switch tok.type {
	case .Inner:
		src.join_type = .Inner
	case .Left:
		src.join_type = .Left
	case .Right:
		src.join_type = .Right
	case .Full:
		src.join_type = .Full
	case .Cross:
		src.join_type = .Cross
	case:
		return .Error
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
	if sql.branch_state != .Expect_Else {
		return
	}

	q := _get_curr_query(sql)

	l := &sql.listener
	branch_query := sql.listener.query_stack[len(l.query_stack) - 2]

	branch := &branch_query.operation.(Branch)

	op, ok := q.operation.(Branch)

	/* Naked else */
	if !ok || op.type  == .While {
		branch.else_scope = i32(len(sql.scopes))
		append(&sql.scopes, make_scope())
		
		s := &sql.scopes[len(sql.scopes) - 1]

		bs := &sql.scopes[branch.scope]
		s.parent = bs.parent
		sql.curr_scope = branch.else_scope
		return
	}

	/* If we made it this far, we have entered an "else if" */
	sql.branch_state = .Expect_Expr
	else_if_stmt := sql.queries[len(sql.queries) -1]
	else_if := else_if_stmt.operation.(Branch)
	else_if.type = .Else_If
}

@(private = "file")
_get_curr_query :: proc(sql: ^Streamql) -> ^Query {
	l := &sql.listener
	if len(l.query_stack) == 0 {
		return nil
	}
	return l.query_stack[len(l.query_stack) - 1]
}
