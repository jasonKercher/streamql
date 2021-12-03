package streamql

import "core:fmt"
import "core:os"

import "core:testing"

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

@(private)
_get_next_token :: proc {_get_next_token_from_here, _get_next_token_from_curr}

@(private)
_get_next_token_from_here :: proc(self: ^Sql_Parser, here: ^u32) -> bool {
	i := here^ + 1
	if i >= u32(len(self.tokens)) {
		return true
	}
	for ; self.tokens[i].type != .Query_End && self.tokens[i].type == .Query_Comment; i += 1 {}
	here^ = i
	return self.tokens[i].type == .Query_End
}

@(private)
_get_next_token_from_curr :: proc(self: ^Sql_Parser) -> bool {
	i := self.curr + 1
	if i >= u32(len(self.tokens)) {
		return true
	}
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
		next_idx := _peek_next_token(self, begin)
		if self.tokens[next_idx].type == .Sym_Dot {
			next_idx = _peek_next_token(self, next_idx)
			field_name_tok := &self.tokens[next_idx]
			if field_name_tok.type != .Query_Name {
				return parse_error(self, "unexpected token")
			}
			return parse_send_name(self, field_name_tok, tok)
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
_skip_between_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
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
_parse_expression :: proc(self: ^Sql_Parser, begin, end: u32, min_group: int) -> Sql_Result {
	/* If we don't accomplish anything, someone wrote something
	 * dumb like `select (1+1)`. We will need to re-enter this
	 * function with min_group set to -1
	 */
	did_some_breakdown: bool

	curr_group := self.tokens[begin].group
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
	for i := begin; i < end; i += 1 {
		if self.tokens[i].done || self.tokens[i].group != curr_group {
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
	for i := begin; i < end; i += 1 {
		if self.tokens[i].done || self.tokens[i].group != curr_group {
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
	for i := begin; i < end; i += 1 {
		if self.tokens[i].done || self.tokens[i].group != curr_group {
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
	for i := begin; i < end; i += 1 {
		if self.tokens[i].done || self.tokens[i].group != curr_group {
			continue
		}
		if self.tokens[i].type == .Case {
			did_some_breakdown = true
			self.tokens[i].done = true
			_parse_case_stmt(self) or_return
		}
	}

	/* functions */
	for i := begin; i != end; i += 1 {
		if self.tokens[i].done || self.tokens[i].group != curr_group {
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
_find_expression :: proc(self: ^Sql_Parser, allow_star: bool) -> (level: int, ret: Sql_Result) {
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

	lowest_exit_level : int
	first_token_vec := make([dynamic]u32)
	defer delete(first_token_vec)

	for self.tokens[self.curr].type != .Query_End  {
		append(&first_token_vec, self.curr)
		if self.tokens[self.curr].type == .Sym_Lparen {
			next := _peek_next_token(self, self.curr)
			/* if this is a subquery const, we want to
			 * break out of this loop all together
			 */
			if self.tokens[self.curr].type == .Select {
				break
			}
			level += 1
			lowest_exit_level += 1
		} else {
			break
		}
	}

	state := _Expr_State.Expect_Val
	min_group := 10000 /* lol */
	in_expr := true
	begin := self.curr

	/* Expressions require 2 passes. This is the first pass
	 * where we loop through each token and assign it a group.
	 * The group is based on the level of parentheses.
	 */
	for self.tokens[self.curr].type != .Query_End {
		next_state := _Expr_State.None
		switch state {
		case .Expect_Val:
			may_be_function := true

			#partial switch self.tokens[self.curr].type {
			case .Sym_Multiply:
				if (!allow_star) {
					return 0, parse_error(self, "unexpected token")
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
			#partial switch self.tokens[self.curr].type {
			case .Sym_Rparen:
				if level == 0 {
					in_expr = false
				} else {
					if level-1 < lowest_exit_level {
						level -= 1
						lowest_exit_level -= 1
						pop(&first_token_vec)
					}
					next_state = .Expect_Op_Or_End
				}
			case .Sym_Dot:
				fallthrough
			case .Sym_Plus:
				fallthrough
			case .Sym_Minus:
				fallthrough
			case .Sym_Multiply:
				fallthrough
			case .Sym_Divide:
				fallthrough
			case .Sym_Modulus:
				fallthrough
			case .Sym_Bit_Not_Unary:
				fallthrough
			case .Sym_Bit_Or:
				fallthrough
			case .Sym_Bit_And:
				fallthrough
			case .Sym_Bit_Xor:
				next_state = .Expect_Val
			case:
				next_state = state
				in_expr = false
			}
		case .In_Function:
			return 0, parse_error(self, "function expression incomplete")
		case .In_Case:
			return 0, parse_error(self, "case expressions incomplete")
		case .None:
			in_expr = false
		}

		if !in_expr {
			break
		}

		state = next_state

		if int(self.tokens[self.curr].group) < min_group &&
		    self.tokens[self.curr].type != .Sym_Lparen &&
		    self.tokens[self.curr].type != .Sym_Rparen {
			min_group = int(self.tokens[self.curr].group)
		}

		_get_next_token(self)
	}

	begin = pop(&first_token_vec)
	self.tokens[begin].end_expr = self.curr

	if state != .Expect_Op_Or_End || level != lowest_exit_level {
		return 0, parse_error(self, "unexpected end of expression")
	}

	/* Only used when calling _parse_expression non-recursively */
	self.tokens[begin].min_grp = u32(min_group)

	return level, .Ok
}

/* Should not discover any syntax errors at this point */
@(private)
_is_a_single_boolean_expression :: proc (self: ^Sql_Parser, begin, end: u32) -> bool {
	begin := begin

	/* Find beginning of left side expression */
	for ; begin < end && self.tokens[begin].end_expr == 0; begin += 1 { }

	begin = self.tokens[begin].end_expr - 1 /* Back the fuck up */

	_get_next_token(self, &begin)

	/* We should now be ON an comparison operator */
	#partial switch self.tokens[begin].type {
	case .Sym_Eq:
		fallthrough
	case .Sym_Ne:
		fallthrough
	case .Sym_Gt:
		fallthrough
	case .Sym_Ge:
		fallthrough
	case .Sym_Lt:
		fallthrough
	case .Sym_Le:
		fallthrough
	case .Like:
		_get_next_token(self, &begin)
		if self.tokens[begin].end_expr == 0 {
			return false
		}
	case .In:
		fallthrough
	case .Between:
		/* TODO */
		return false
	case:
		return false
	}

	return self.tokens[begin].end_expr >= end
}

@(private)
_parse_send_predicate :: proc(self: ^Sql_Parser, begin: u32) -> Sql_Result {
	begin := begin

	/* Find beginning of left side expression */
	for ; self.tokens[begin].end_expr == 0; begin += 1 {}

	left := begin
	begin = self.tokens[begin].end_expr - 1; /* Back the fuck up */

	_get_next_token(self, &begin)
	oper := begin

	/* We should now be ON an comparison operator */
	#partial switch self.tokens[oper].type {
	case .Sym_Eq:
		fallthrough
	case .Sym_Ne:
		fallthrough
	case .Sym_Gt:
		fallthrough
	case .Sym_Ge:
		fallthrough
	case .Sym_Lt:
		fallthrough
	case .Sym_Le:
		fallthrough
	case .Like:
		parse_enter_predicate(self, &self.tokens[oper])
		_get_next_token(self, &begin); /* begin now pointing at right side expr */
		left_tok := self.tokens[left]
		begin_tok := self.tokens[begin]
		_parse_expression(self, left, left_tok.end_expr, int(left_tok.min_grp)) or_return
		_parse_expression(self, begin, begin_tok.end_expr, int(begin_tok.min_grp)) or_return
		parse_leave_predicate(self, &self.tokens[oper])
	case .In:
		fallthrough
	case .Between:
		/* TODO */
		return parse_error(self, "in/between current incomplete")
	case:
		return parse_error(self, "unexpected token in predicate")
	}

	return .Ok
}

@(private)
_parse_boolean_expression :: proc(self: ^Sql_Parser, begin, end: u32, min_group: int) -> Sql_Result {
	/* If we don't accomplish anything, someone wrote
	 * something dumb like `select 1 where (1=1)`. We
	 * will need to re-enter this function with min_group
	 * set to -1.
	 */
	did_some_breakdown : bool

	curr_group := self.tokens[begin].group
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

	/* Shortcut for leaf node */
	if _is_a_single_boolean_expression(self, begin, end) {
		return _parse_send_predicate(self, begin)
	}


	/* First, split on OR */
	for i := begin; i != end; i += 1 {
		if self.tokens[i].done || self.tokens[i].group != curr_group {
			continue
		}
		#partial switch self.tokens[i].type {
		case .Or:
			did_some_breakdown = true
			self.tokens[i].done = true
			parse_enter_or(self)
			_parse_boolean_expression(self, begin, i, -1) or_return
			_parse_boolean_expression(self, i + 1, end, -1) or_return
			parse_leave_or(self)
		case:
		}
	}

	/* Split on AND */
	for i := begin; i != end; i += 1 {
		if self.tokens[i].done || self.tokens[i].group != curr_group {
			continue
		}
		#partial switch self.tokens[i].type {
		case .And:
			did_some_breakdown = true
			self.tokens[i].done = true
			parse_enter_or(self)
			_parse_boolean_expression(self, begin, i, -1) or_return
			_parse_boolean_expression(self, i + 1, end, -1) or_return
			parse_leave_or(self)
		case:
		}
	}

	if !did_some_breakdown {
		return _parse_boolean_expression(self, begin, end, -1)
	}

	return .Ok
}

/* TODO: convert parse_expression to have a skip option, because
 *       this becomes quite difficult when I don't know where an
 *       expression ends...
 */
@(private)
_bool_state :: enum {
	Expect_Logic_Or_End,
	Expect_Expression,
	Expect_Comparison,
}

@(private)
_find_boolean_expression :: proc(self: ^Sql_Parser) -> Sql_Result {
	state := _bool_state.Expect_Expression

	begin := self.curr

	level : int
	min_group := 10000
	left_side := true

	/* This is very similar to the first pass of _find_expression
	 * except, there is no real "interpretation" here. The only
	 * purpose of this pass is to split the logic based on the
	 * following precedecnce:  (), AND, OR
	 */
	loop: for self.tokens[self.curr].type != .Query_End {
		switch state {
		case .Expect_Expression:
			curr_token := self.curr
			extra_level := _find_expression(self, false) or_return
			for ; extra_level != 0; curr_token -= 1 {
				level += 1
				extra_level -= 1
			}
			if left_side {
				state = .Expect_Comparison
			} else {
				state = .Expect_Logic_Or_End
			}
			left_side = !left_side
		case .Expect_Comparison:
			#partial switch self.tokens[self.curr].type {
			case .Sym_Eq:
				fallthrough
			case .Sym_Ne:
				fallthrough
			case .Sym_Lt:
				fallthrough
			case .Sym_Le:
				fallthrough
			case .Sym_Gt:
				fallthrough
			case .Sym_Ge:
				fallthrough
			case .Like:
				_get_next_token_or_die(self) or_return
				state = .Expect_Expression
			case .Between:
				_skip_between_stmt(self) or_return
				state = .Expect_Logic_Or_End
			case .In:
				_skip_expression_list(self) or_return
				state = .Expect_Logic_Or_End
			case:
				return parse_error(self, "Unexpected token")
			}
		case .Expect_Logic_Or_End:
			#partial switch self.tokens[self.curr].type {
			case .Sym_Rparen:
				level -= 1
			case .And:
				fallthrough
			case .Or:
				state = .Expect_Expression
			case:
				//return parse_error(self, "unexpected token")
				break loop
			}
			_get_next_token(self)
		}

		if int(self.tokens[self.curr].group) < min_group {
			min_group = int(self.tokens[self.curr].group)
		}
	}

	if state != .Expect_Logic_Or_End || level != 0 {
		return parse_error(self, "unexpected end of boolean expression")
	}

	return _parse_boolean_expression(self, begin, self.curr, min_group)
}

@(private)
_parse_execute_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "proc execution incomplete")
}

@(private)
_parse_into_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	_get_next_token_or_die(self) or_return
	parse_send_into_name(self, &self.tokens[self.curr]) or_return
	if _get_next_token(self) {
		return .Ok
	}

	#partial switch self.tokens[self.curr].type {
	case .From:
		return _parse_from_stmt(self)
	case .Where:
		return _parse_where_stmt(self)
	case .Group:
		return _parse_groupby_stmt(self)
	case .Having:
		return _parse_having_stmt(self)
	case .End_Of_Subquery:
		return .Ok
	case:
		return _parse_enter(self)
	}
}

@(private)
_parse_subquery_source :: proc(self: ^Sql_Parser) -> Sql_Result {
	parse_enter_subquery_source(self)

	at_select := self.curr

	/* use this to mark the end of the subquery */
	_skip_subquery(self) or_return
	after_subquery := self.curr

	/* prepare to parse subquery */
	self.curr = at_select

	ret := _parse_select_stmt(self)

	/* ready to begin parsing higher level query now */
	self.curr = after_subquery

	parse_leave_subquery_source(self)
	return ret
}

@(private)
_parse_source_item :: proc(self: ^Sql_Parser) -> Sql_Result {
	_get_next_token_or_die(self) or_return

	/* Parse source name or subquery source */
	#partial switch self.tokens[self.curr].type {
	case .Query_Name:
		fallthrough
	case .Query_Variable:
		parse_send_table_source(self, &self.tokens[self.curr])
	case .Sym_Lparen:
		_get_next_token_or_die(self) or_return
		if (self.tokens[self.curr].type != .Select) {
			return parse_error(self, "expected subquery")
		}
		_parse_subquery_source(self)
	case:
		return parse_error(self, "unexpected token")
	}

	if _get_next_token(self) {
		return .Ok
	}

	/* check for alias */
	if self.tokens[self.curr].type == .As {
		_get_next_token_or_die(self) or_return
	}

	if self.tokens[self.curr].type == .Query_Name ||
	    self.tokens[self.curr].type == .Query_Variable {
		parse_send_source_alias(self, &self.tokens[self.curr])
		if (_get_next_token(self)) {
			return .Ok
		}
	}

	return .Ok
}

@(private)
_parse_from_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	parse_enter_from(self)
	in_source_list := true

	_parse_source_item(self) or_return

	/* Condition currently redundant */
	for in_source_list {
		join_type_idx := self.curr
		expect_on := false

		/* check for join */
		#partial switch self.tokens[join_type_idx].type {
		case .Inner:
			fallthrough
		case .Left:
			fallthrough
		case .Right:
			fallthrough
		case .Full:
			expect_on = true
			fallthrough
		case .Cross:
			_get_next_token_or_die(self) or_return
			if self.tokens[self.curr].type != .Join {
				return parse_error(self, "expected JOIN")
			}
		case .Join:
			self.tokens[join_type_idx].type = .Inner
			expect_on = true
		case .Sym_Comma: /* cross join */
			self.tokens[join_type_idx].type = .Cross
		case:
			in_source_list = false
		}

		if !in_source_list {
			break
		}

		parse_send_join_type(self, &self.tokens[join_type_idx]) or_return
		_parse_source_item(self) or_return

		if expect_on {
			if self.tokens[self.curr].type != .On {
				return parse_error(self, "expected ON")
			}
			_get_next_token_or_die(self) or_return
			_find_boolean_expression(self) or_return
		}
	}

	parse_leave_from(self)

	#partial switch self.tokens[self.curr].type {
	case .Where:
		return _parse_where_stmt(self)
	case .Group:
		return _parse_groupby_stmt(self)
	case .Having:
		return _parse_having_stmt(self)
	case .End_Of_Subquery:
		return .Ok
	case:
		return _parse_enter(self)
	}
}

@(private)
_parse_where_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	_get_next_token_or_die(self) or_return
	return _find_boolean_expression(self)
}

@(private)
_parse_groupby_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "group by incomplete")
}

@(private)
_parse_having_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "having incomplete")
}

@(private)
_parse_select_list :: proc(self: ^Sql_Parser) -> Sql_Result {
	for {
		expr_begin := self.curr
		extra_level := _find_expression(self, true) or_return
		if extra_level > 0 {
			return parse_error(self, "unmatched '('")
		}
		_parse_expression(self,
		                  expr_begin,
		                  self.tokens[expr_begin].end_expr,
		                  int(self.tokens[expr_begin].min_grp)) or_return

		if self.tokens[self.curr].type == .As {
			_get_next_token_or_die(self) or_return
		}

		if self.tokens[self.curr].type == .Query_Name {
			parse_send_column_alias(self, &self.tokens[self.curr])
			if _get_next_token(self) {
				return .Ok
			}
		}

		#partial switch self.tokens[self.curr].type {
		case .Sym_Comma:
			_get_next_token_or_die(self) or_return
		case .Into:
			return _parse_into_stmt(self)
		case .From:
			return _parse_from_stmt(self)
		case .Where:
			return _parse_where_stmt(self)
		case .Group:
			return _parse_groupby_stmt(self)
		case .Having:
			return _parse_having_stmt(self)
		case .End_Of_Subquery:
			return .Ok
		case:
			return _parse_enter(self)
		}
	}
	return .Ok
}


@(private)
_parse_select_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	parse_send_select_stmt(self)
	_get_next_token_or_die(self) or_return

	all_or_distinct_allowed := true
	top_allowed := true

	for {
		#partial switch self.tokens[self.curr].type {
		case .All:
			if !all_or_distinct_allowed {
				return parse_error(self, "unexpected token")
			}
			/* This is really a no-op anyway... */
			parse_send_all(self)
			all_or_distinct_allowed = false
		case .Distinct:
			if !all_or_distinct_allowed {
				return parse_error(self, "unexpected token")
			}
			parse_send_distinct(self)
			all_or_distinct_allowed = false
		case .Top:
			if !top_allowed {
				return parse_error(self, "unexpected token")
			}
			_get_next_token_or_die(self) or_return
			parse_enter_top_expr(self)
			expr_begin := self.curr
			
			extra_level := _find_expression(self, false) or_return
			if extra_level > 0 {
				return parse_error(self, "unmatched '('")
			}
			_parse_expression(self,
			                  expr_begin,
			                  self.tokens[expr_begin].end_expr,
			                  int(self.tokens[expr_begin].min_grp)) or_return
			parse_leave_top_expr(self)
			all_or_distinct_allowed = false
			top_allowed = false
		case:
			return _parse_select_list(self)
		}
	}
	return .Ok
}

@(private)
_parse_delete_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "dead end")
}

@(private)
_parse_update_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "dead end")
}

@(private)
_parse_insert_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_alter_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_create_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_drop_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_truncate_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_break_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_continue_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_goto_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_if_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "dead end")
}

@(private)
_parse_return_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_waitfor_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_while_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_print_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_raiserror_stmt :: proc(self: ^Sql_Parser) -> Sql_Result {
	return parse_error(self, "not implemented")
}

@(private)
_parse_enter :: proc(self: ^Sql_Parser) -> Sql_Result {
	if (self.tokens[self.curr].type == .Query_End) {
		return .Ok
	}
	#partial switch self.tokens[self.curr].type {
	case .Query_Name:
		return _parse_execute_stmt(self)
	case .Select:
		return _parse_select_stmt(self)
	case .Delete:
		return _parse_delete_stmt(self)
	case .Update:
		return _parse_update_stmt(self)
	case .Insert:
		return _parse_insert_stmt(self)
	case .Alter:
		return _parse_alter_stmt(self)
	case .Create:
		return _parse_create_stmt(self)
	case .Drop:
		return _parse_drop_stmt(self)
	case .Truncate:
		return _parse_truncate_stmt(self)
	case .Break:
		return _parse_break_stmt(self)
	case .Continue:
		return _parse_continue_stmt(self)
	case .Goto:
		return _parse_goto_stmt(self)
	case .If:
		return _parse_if_stmt(self)
	case .Return:
		return _parse_return_stmt(self)
	case .Waitfor:
		return _parse_waitfor_stmt(self)
	case .While:
		return _parse_while_stmt(self)
	case .Print:
		return _parse_print_stmt(self)
	case .Raiserror:
		return _parse_raiserror_stmt(self)
	case:
		return parse_error(self, "unexpected token")
	}
	return .Ok
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
	error_tok := self.tokens[self.curr]
	error_str := self.q[error_tok.begin:error_tok.begin+error_tok.len]
	line, off := parse_get_pos(self, self.tokens[self.curr].begin)

	fmt.fprintf(os.stderr, "%s at `%s': (line: %d, pos: %d)\n", msg, error_str, line, off)
	return .Error
}

parse_parse :: proc(self: ^Sql_Parser, query_str: string) -> Sql_Result {
	self.q = query_str
	lex_lex(self)

	self.curr = 0
	if (_get_next_token(self)) {
		return .Ok
	}

	return _parse_enter(self)
}

@(test)
parse_test :: proc (t: ^testing.T) {
	parser: Sql_Parser
	parse_init(&parser)
	parse_parse(&parser, "select 1")
	//testing.error(t, "shit!")
}
