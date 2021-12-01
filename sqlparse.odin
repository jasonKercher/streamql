package streamql

import "core:fmt"
import "core:os"

Sql_Parser :: struct {
	q:       string,
	lf_vec:  [dynamic]u32,
	tokens:  [dynamic]Token,
	tok_map: map[string]Token_Type,
	curr:    u32,
}

Func_Group :: enum {
	None,
	Windowed,
	Aggregate,
	Scalar,
}

parse_init :: proc(self: ^Sql_Parser) {
	/* Current length of token map is 185. Looks like the maps resize
	 * at 3/4 full, so that puts us at 247 in order to not resize.
	 */
	self^ = {
		lf_vec = make([dynamic]u32),
		tokens = make([dynamic]Token),
		tok_map = make(map[string]Token_Type, 256),
	}
}

parse_destroy :: proc(self: ^Sql_Parser) {
	delete(self.lf_vec)
	delete(self.tokens)
	delete(self.tok_map)
}

parse_get_pos :: proc(self: ^Sql_Parser, idx: u32) -> (line, off: u32) {
	line = 1
	for lf in self.lf_vec {
		if lf > idx {
			break
		}
		off = lf
		line += 1
	}
	off = idx - off
	return
}

token_get_pos :: proc(self: ^Sql_Parser, tok: ^Token) -> (line, off: u32) {
	return parse_get_pos(self, tok.begin)
}

token_to_string :: proc(self: ^Sql_Parser, tok: ^Token) -> string {
	return self.q[tok.begin:tok.begin+tok.len]
}


/********/

parse_error :: proc(self: ^Sql_Parser, msg: string) -> Sql_Result {
	line, off := parse_get_pos(self, self.tokens[self.curr].begin)
	fmt.fprintf(os.stderr, "%s: (line: %d, pos: %d)\n", msg, line, off)
	return .Error
}

@(private)
_get_next_token :: proc(self: ^Sql_Parser) -> bool {
	i := self.curr + 1
	for ; self.tokens[i].type != .Query_End && self.tokens[i].type == .Query_Comment; i += 1 {}
	self.curr = i
	return self.tokens[i].type == .Query_End
}

@(private)
_peek_next_token :: proc(self: ^Sql_Parser, i: u32) -> u32 {
	i := i + 1
	for ; self.tokens[i].type != .Query_End && self.tokens[i].type == .Query_Comment; i += 1 {}
	return i
}

@(private)
_get_next_token_or_die :: proc(self: ^Sql_Parser) -> Sql_Result {
	if _get_next_token(self) {
		return parse_error(self, "unexpected EOF")
	}
	return .Ok
}

@(private)
_get_func_group :: proc(type: Token_Type) -> Func_Group {
	switch int(type) {
	case 300..399:
		return .Windowed
	case 400..499:
		return .Aggregate
	case 500..599:
		return .Scalar
	}
	return .None
}

@(private)
_send_column_or_const :: proc(self: ^Sql_Parser, begin: u32) -> Sql_Result {
	tok := &self.tokens[begin]
	#partial switch tok.type {
	case .Sym_Asterisk:
		return parse_error(self, "send asterisk incomplete")
	case .Query_Name:
		next_idx := _peek_next_token(self, self.curr)
		if self.tokens[next_idx].type == .Sym_Dot {
			next_idx = _peek_next_token(self, next_idx)
			table_name_tok := &self.tokens[next_idx]
			if table_name_tok.type != .Query_Name {
				return parse_error(self, "unexpected token")
			}
			return parse_send_name(self, tok, table_name_tok)
		}
		return parse_send_name(self, tok, nil)
	case .Literal_Int:
		return parse_send_int(self, tok)
	case .Literal_Float:
		return parse_send_float(self, tok)
	case .Literal_String:
		return parse_send_string(self, tok)
	case:
		return parse_error(self, "unexpected token")
	}
}

@(private)
_parse_case_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "case statement parsing incomplete")
}

@(private)
_parse_function :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "function parsing incomplete")
}

@(private)
_skip_between_statement :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "between skipping incomplete")
}

@(private)
_skip_expression_list :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "expression list skipping incomplete")
}

@(private)
_skip_subquery :: proc(self: ^Sql_Parser) -> Sql_Result {
	level := 1
	for level != 0 {
		_get_next_token_or_die(self) or_return
		#partial switch self.tokens[self.curr].type {
		case .Sym_Lparen:
			level += 1
		case .End_Of_Subquery:
			level -= 1
		case .Sym_Rparen:
			level -= 1
		case:
		}
	}

	if (level > 0) {
		return parse_error(self, "failed to parse subquery")
	}

	self.tokens[self.curr].type = .End_Of_Subquery
	return .Ok
}

@(private)
_is_a_single_term :: proc(self: ^Sql_Parser, begin, end: u32) -> bool {
	term_found: bool
	has_table_name: bool

	for begin:= begin; begin != end; begin += 1 {
		#partial switch self.tokens[begin].type {
		case .Query_Comment:
			continue
		case .Sym_Dot:
			if (!term_found) {
				return false
			}
			has_table_name = true
		case .Sym_Asterisk:
			fallthrough
		case .Query_Name:
			fallthrough
		case .Query_Variable:
			fallthrough
		case .Literal_Int:
			fallthrough
		case .Literal_Float:
			fallthrough
		case .Literal_String:
			if (term_found && !has_table_name) {
				return false
			}
			term_found = true
			has_table_name = false
		case:
			return false
		}
	}

	return true
}

@(private)
_parse_expression :: proc(self: ^Sql_Parser, begin, end: u32, min_group: i32) -> Sql_Result {
	/* If we don't accomplish anything, someone wrote something
	 * dumb like `select (1+1)`. We will need to re-enter this
	 * function with min_group set to -1
	 */
	 did_some_breakdown: bool

	curr_group : u32 = self.tokens[begin].grp
	if min_group != -1 {
		curr_group = u32(min_group)
	}

	begin := begin
	end := end

	for self.tokens[begin].type == .Sym_Lparen || self.tokens[begin].type == .Query_Comment {
		begin += 1
	}

	for self.tokens[end].type == .Sym_Rparen || self.tokens[end].type == .Query_Comment {
		end -= 1
	}

	if self.tokens[begin].type == .Select {
		parse_enter_subquery_const(self)

		curr_bak := self.curr
		self.curr = begin

		ret := _parse_select_stmt(self)

		self.curr = curr_bak
		parse_leave_subquery_const(self)
		return ret
	}

	/* Leaf node?? */
	if _is_a_single_term(self, begin, end) {
		return _send_column_or_const(self, begin)
	}

	/* Lowest precedence first */
	for i := begin; begin != end; i += 1 {
		if self.tokens[i].done || self.tokens[i].grp != curr_group {
			continue
		}
		#partial switch self.tokens[i].type {
		case .Sym_Plus:
			fallthrough
		case .Sym_Minus:
			fallthrough
		case .Sym_Bit_And:
			fallthrough
		case .Sym_Bit_Or:
			fallthrough
		case .Sym_Bit_Xor:
			did_some_breakdown = true
			self.tokens[i].done = true
			parse_enter_function(self, &self.tokens[i])
			_parse_expression(self, begin, i, -1) or_return
			_parse_expression(self, i+1, end, -1) or_return
			parse_leave_function(self, &self.tokens[i])
		case:
		}
	}
	
	/* multiplication derivatives */
	for i := begin; begin != end; i += 1 {
		if self.tokens[i].done || self.tokens[i].grp != curr_group {
			continue
		}
		#partial switch self.tokens[i].type {
		case .Sym_Multiply:
			fallthrough
		case .Sym_Divide:
			fallthrough
		case .Sym_Modulus:
			did_some_breakdown = true
			self.tokens[i].done = true
			parse_enter_function(self, &self.tokens[i])
			_parse_expression(self, begin, i, -1) or_return
			_parse_expression(self, i+1, end, -1) or_return
			parse_leave_function(self, &self.tokens[i])
		case:
		}
	}

	/* unary expressions */
	for i := begin; begin != end; i += 1 {
		if self.tokens[i].done || self.tokens[i].grp != curr_group {
			continue
		}
		#partial switch self.tokens[i].type {
		case .Sym_Plus_Unary:
			fallthrough
		case .Sym_Minus_Unary:
			fallthrough
		case .Sym_Bit_Not_Unary:
			did_some_breakdown = true
			self.tokens[i].done = true
			parse_enter_function(self, &self.tokens[i])
			_parse_expression(self, i+1, end, -1) or_return
			parse_leave_function(self, &self.tokens[i])
		case:
		}
	}

	/* case expressions */
	for i := begin; begin != end; i += 1 {
		if self.tokens[i].done || self.tokens[i].grp != curr_group {
			continue
		}
		if self.tokens[i].type == .Case {
			did_some_breakdown = true
			self.tokens[i].done = true
			_parse_case_stmt(self) or_return
		}
	}

	/* functions */
	for i := begin; begin != end; i += 1 {
		if self.tokens[i].done || self.tokens[i].grp != curr_group {
			continue
		}
		if self.tokens[i].type == .Case {
			did_some_breakdown = true
			self.tokens[i].done = true
			_parse_function(self) or_return
		}
	}

	if !did_some_breakdown {
		return _parse_expression(self, begin, end, -1)
	}

	return .Ok
}

@(private)
_Expr_State :: enum {
	None,
	Expect_Op_Or_End,
	Expect_Val,
	In_Function,
	In_Case,
}

@(private)
_find_expression :: proc(self: ^Sql_Parser, allow_star: bool) -> Sql_Result {
	/* This variable represents the lowest legal level
	 * for exiting the expression.  It is required for
	 * expressions like:
	 *
	 * SELECT 1
	 * WHERE (1+1)=2 AND 3=3
	 *       ^~~ We don't (currently) know if this '('
	 *           belongs to the expression or not. Here 
	 *           it does, but...
	 *
	 * SELECT 1
	 * WHERE (1+1 = 2 OR 3=3) AND 4=4
	 *       ^~~ ...here, it does not.
	 */

	level : int
	lowest_exit_level : int
	first_token_vec := make([dynamic]^Token)
	defer delete(first_token_vec)

	for self.tokens[self.curr].type != .Query_End  {
		append(&first_token_vec, &self.tokens[self.curr])
		if self.tokens[self.curr].type == .Sym_Lparen {
			next := _peek_next_token(self, self.curr)
			/* if this is a subquery const, we want to
			 * break out of this loop all together
			 */
			if self.tokens[self.curr].type == .Select {
				break;
			}
			level += 1
			lowest_exit_level += 1
		} else {
			break
		}
		_get_next_token(self)
	}
	_get_next_token(self) /* */

	state := _Expr_State.Expect_Val
	min_group := 10000 /* lol */
	in_expr := true
	begin := &self.tokens[self.curr]

	/* Expressions require 2 passes. This is the first pass
	 * where we loop through each token and assign it a group.
	 * The group is based on the level of parentheses.
	 */
	for ; self.tokens[self.curr].type != .Query_End; {
		next_state := _Expr_State.None
		switch state {
		case .Expect_Val:
			may_be_function := true

			#partial switch self.tokens[self.curr].type {
			case .Sym_Multiply:
				if (!allow_star) {
					return parse_error(self, "unexpected token")
				}
				self.tokens[self.curr].type = .Sym_Asterisk
				next_state = .Expect_Op_Or_End
				may_be_function = false
			case .Sym_Plus:
				self.tokens[self.curr].type = .Sym_Plus_Unary
				next_state = .Expect_Val
				may_be_function = false
			case .Sym_Minus:
				self.tokens[self.curr].type = .Sym_Minus_Unary
				next_state = .Expect_Val
				may_be_function = false
			case .Sym_Bit_Not_Unary:
				next_state = .Expect_Val
				may_be_function = false
			case .Case:
				next_state = .In_Case
				may_be_function = false
			case .Query_Name .. .Literal_String:
				next_state = .Expect_Op_Or_End
				may_be_function = false
			case .Sym_Lparen:
				may_be_function = false
				next := _peek_next_token(self, self.curr)
				/* if this is a Subquery Const, we need to skip over
				 * all the tokens belonging to the subquery...
				 */
				if self.tokens[next].type == .Select {
					next_state = .Expect_Op_Or_End
					_skip_subquery(self) or_return
				} else {
					level += 1
					next_state = .Expect_Val
				}
			case:
				in_expr = false
			}

			if (may_be_function) {
				fn_group := _get_func_group(self.tokens[self.curr].type)
				next := _peek_next_token(self, self.curr)
				if self.tokens[next].type != .Sym_Lparen {
					/* If next token isn't '(' then treat as name */
					self.tokens[self.curr].type = .Query_Name
					next_state = .Expect_Op_Or_End
				} else {
					next_state = .In_Function
					level += 1
				}
			}
		case .Expect_Op_Or_End:
			fallthrough
		case .In_Function:
			fallthrough
		case .In_Case:
			fallthrough
		case .None:
		}
		_get_next_token(self) 
	}

	_get_next_token(self)  /* */

	return .Ok
}

@(private)
_parse_select_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return .Ok
}

@(private)
_parse_enter :: proc(self: ^Sql_Parser) -> Sql_Result {
	return .Ok
}

parse_parse :: proc(self: ^Sql_Parser, query_str: string) -> Sql_Result {
	self.q = query_str
	lex_lex(self)

	self.curr = 0
	if (_get_next_token(self)) {
		return .Ok
	}

	return _parse_enter(self);
}


