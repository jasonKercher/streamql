package streamql

import "core:os"
import "core:fmt"
import "core:strings"
import "core:unicode"
import "core:testing"
import "core:container/bit_array"

Token :: struct {
	begin:    u32,
	len:      u32,
	end_expr: u32,
	group:    u16,
	type:     Token_Type,
}

Token_Type :: enum u16 {
	/* Not Keywords */
	Query_Begin,    /* All parsing begins here... */
	Query_End,      /*    ... and ends here       */
	Query_Comment,
	Query_Name = 20,
	Query_Variable,
	Literal_Int,
	Literal_Float,
	Literal_String,

	/* Special un-mapped tokens */
	Sym_Asterisk = 50,  /* was Sym_Multiply */
	Sym_Plus_Unary,     /* was Sym_Plus */
	Sym_Minus_Unary,    /* was Sym_Minus */
	End_Of_Subquery,    /* was Sym_Rparen */

	/* Keywords */
	Day = 100,
	Month,
	Sign,
	Str,
	Year,
	Text,
	Ntext,
	Add,
	All,
	Alter,
	And,
	Any,
	As,
	Begin,
	Between,
	Break,
	By,
	Case,
	Column,
	Continue,
	Create,
	Cross,
	Declare,
	Delete,
	Desc,
	Distinct,
	Distributed,
	Drop,
	Else,
	End,
	Execute,
	Exists,
	From,
	Full,
	Function,
	Goto,
	Group,
	Having,
	If,
	In,
	Inner,
	Insert,
	Into,
	Is,
	Join,
	Like,
	Not,
	Null,
	Of,
	Off,
	On,
	Open,
	Or,
	Order,
	Over,
	Percent,
	Print,
	Proc,
	Procedure,
	Raiserror,
	Replication,
	Return,
	Revert,
	Rollback,
	Save,
	Schema,
	Select,
	Set,
	Table,
	Then,
	To,
	Top,
	Tran,
	Transaction,
	Truncate,
	Union,
	Update,
	User,
	Values,
	When,
	Where,
	While,
	Bigint,
	Checksum,
	Days,
	Go,
	Hash,
	Hours,
	Int,
	Minutes,
	Range,
	Row,
	Rows,
	Seconds,
	Smallint,
	Static,
	Statusonly,
	Tinyint,
	Wait,
	Waitfor,
	Varchar,
	Nvarchar,

	/* Windowed Functions */
	Dense_Rank = 300,
	Rank,
	Row_Number,

	/* Aggregate Functions */
	Avg = 400,
	Checksum_Agg,
	Count,
	Max,
	Min,
	Stdev,
	Stdevp,
	String_Agg,
	Sum,

	/* Scalar Functions */
	Abs = 500,
	Ascii,
	Cast,
	Ceiling,
	Char,
	Charindex,
	Coalesce,
	Concat,
	Convert,
	Datalength,
	Dateadd,
	Datediff,
	Datename,
	Datepart,
	Floor,
	Getdate,
	Getutcdate,
	Isdate,
	Isnull,
	Isnumeric,
	Left,
	Len,
	Lower,
	Ltrim,
	Nchar,
	Nullif,
	Patindex,
	Rand,
	Replace,
	Right,
	Round,
	Rtrim,
	Space,
	Stuff,
	Substring,
	Try_Cast,
	Upper,
	User_Name,

	/* symbols */
	Sym_Pound = 600,
	Sym_Lparen,
	Sym_Rparen,
	Sym_Plus_Assign,
	Sym_Minus_Assign,
	Sym_Multiply_Assign,
	Sym_Divide_Assign,
	Sym_Modulus_Assign,
	Sym_Bit_Not_Assign,
	Sym_Bit_Or_Assign,
	Sym_Bit_And_Assign,
	Sym_Bit_Xor_Assign,
	Sym_Plus,
	Sym_Minus,
	Sym_Multiply,
	Sym_Divide,
	Sym_Modulus,
	Sym_Bit_Not_Unary,
	Sym_Bit_Or,
	Sym_Bit_And,
	Sym_Bit_Xor,
	Sym_Dot,
	Sym_Eq,
	Sym_Ne,
	Sym_Gt,
	Sym_Ge,
	Sym_Lt,
	Sym_Le,
	Sym_Comma,
	Sym_Semicolon,
	Sym_Block_Comment,
	Sym_Line_Comment,
}

lex_lex :: proc (self: ^Parser) -> Result {
	if len(self.tok_map) == 0 {
		_init_map(self)
	}

	resize(&self.tokens, 0)
	resize(&self.lf_vec, 0)
	bit_array.clear(&self.consumed)

	return _lex_tokenize(self)
}

lex_error :: proc(self: ^Parser, idx: u32, msg: string = "lex error") -> Result {
	line, off := parse_get_pos(self, idx)
	fmt.fprintf(os.stderr, "%s (line: %d, pos: %d)\n", msg, line, off)
	return .Error
}

@(private="file")
_insert_into_map :: proc(self: ^Parser, key: string, type: Token_Type) {
	self.tok_map[key] = type
}

@(private="file")
_init_map :: proc(self: ^Parser) {
	_insert_into_map(self, "abs",          .Abs)
	_insert_into_map(self, "ascii",        .Ascii)
	_insert_into_map(self, "ceiling",      .Ceiling)
	_insert_into_map(self, "char",         .Char)
	_insert_into_map(self, "charindex",    .Charindex)
	_insert_into_map(self, "datalength",   .Datalength)
	_insert_into_map(self, "day",          .Day)
	_insert_into_map(self, "floor",        .Floor)
	_insert_into_map(self, "isdate",       .Isdate)
	_insert_into_map(self, "isnumeric",    .Isnumeric)
	_insert_into_map(self, "len",          .Len)
	_insert_into_map(self, "lower",        .Lower)
	_insert_into_map(self, "ltrim",        .Ltrim)
	_insert_into_map(self, "month",        .Month)
	_insert_into_map(self, "nchar",        .Nchar)
	_insert_into_map(self, "patindex",     .Patindex)
	_insert_into_map(self, "rand",         .Rand)
	_insert_into_map(self, "replace",      .Replace)
	_insert_into_map(self, "round",        .Round)
	_insert_into_map(self, "rtrim",        .Rtrim)
	_insert_into_map(self, "sign",         .Sign)
	_insert_into_map(self, "space",        .Space)
	_insert_into_map(self, "str",          .Str)
	_insert_into_map(self, "substring",    .Substring)
	_insert_into_map(self, "upper",        .Upper)
	_insert_into_map(self, "user_name",    .User_Name)
	_insert_into_map(self, "year",         .Year)
	_insert_into_map(self, "text",         .Text)
	_insert_into_map(self, "ntext",        .Ntext)
	_insert_into_map(self, "add",          .Add)
	_insert_into_map(self, "all",          .All)
	_insert_into_map(self, "alter",        .Alter)
	_insert_into_map(self, "and",          .And)
	_insert_into_map(self, "any",          .Any)
	_insert_into_map(self, "as",           .As)
	_insert_into_map(self, "begin",        .Begin)
	_insert_into_map(self, "between",      .Between)
	_insert_into_map(self, "break",        .Break)
	_insert_into_map(self, "by",           .By)
	_insert_into_map(self, "case",         .Case)
	_insert_into_map(self, "coalesce",     .Coalesce)
	_insert_into_map(self, "column",       .Column)
	_insert_into_map(self, "continue",     .Continue)
	_insert_into_map(self, "convert",      .Convert)
	_insert_into_map(self, "create",       .Create)
	_insert_into_map(self, "cross",        .Cross)
	_insert_into_map(self, "declare",      .Declare)
	_insert_into_map(self, "delete",       .Delete)
	_insert_into_map(self, "desc",         .Desc)
	_insert_into_map(self, "distinct",     .Distinct)
	_insert_into_map(self, "distributed",  .Distributed)
	_insert_into_map(self, "drop",         .Drop)
	_insert_into_map(self, "else",         .Else)
	_insert_into_map(self, "end",          .End)
	_insert_into_map(self, "execute",      .Execute)
	_insert_into_map(self, "exists",       .Exists)
	_insert_into_map(self, "from",         .From)
	_insert_into_map(self, "full",         .Full)
	_insert_into_map(self, "function",     .Function)
	_insert_into_map(self, "goto",         .Goto)
	_insert_into_map(self, "group",        .Group)
	_insert_into_map(self, "having",       .Having)
	_insert_into_map(self, "if",           .If)
	_insert_into_map(self, "in",           .In)
	_insert_into_map(self, "inner",        .Inner)
	_insert_into_map(self, "insert",       .Insert)
	_insert_into_map(self, "into",         .Into)
	_insert_into_map(self, "is",           .Is)
	_insert_into_map(self, "join",         .Join)
	_insert_into_map(self, "left",         .Left)
	_insert_into_map(self, "like",         .Like)
	_insert_into_map(self, "not",          .Not)
	_insert_into_map(self, "null",         .Null)
	_insert_into_map(self, "nullif",       .Nullif)
	_insert_into_map(self, "of",           .Of)
	_insert_into_map(self, "off",          .Off)
	_insert_into_map(self, "on",           .On)
	_insert_into_map(self, "open",         .Open)
	_insert_into_map(self, "or",           .Or)
	_insert_into_map(self, "order",        .Order)
	_insert_into_map(self, "over",         .Over)
	_insert_into_map(self, "percent",      .Percent)
	_insert_into_map(self, "print",        .Print)
	_insert_into_map(self, "proc",         .Proc)
	_insert_into_map(self, "procedure",    .Procedure)
	_insert_into_map(self, "raiserror",    .Raiserror)
	_insert_into_map(self, "replication",  .Replication)
	_insert_into_map(self, "return",       .Return)
	_insert_into_map(self, "revert",       .Revert)
	_insert_into_map(self, "right",        .Right)
	_insert_into_map(self, "rollback",     .Rollback)
	_insert_into_map(self, "save",         .Save)
	_insert_into_map(self, "schema",       .Schema)
	_insert_into_map(self, "select",       .Select)
	_insert_into_map(self, "set",          .Set)
	_insert_into_map(self, "table",        .Table)
	_insert_into_map(self, "then",         .Then)
	_insert_into_map(self, "to",           .To)
	_insert_into_map(self, "top",          .Top)
	_insert_into_map(self, "tran",         .Tran)
	_insert_into_map(self, "transaction",  .Transaction)
	_insert_into_map(self, "truncate",     .Truncate)
	_insert_into_map(self, "union",        .Union)
	_insert_into_map(self, "update",       .Update)
	_insert_into_map(self, "user",         .User)
	_insert_into_map(self, "values",       .Values)
	_insert_into_map(self, "when",         .When)
	_insert_into_map(self, "where",        .Where)
	_insert_into_map(self, "while",        .While)
	_insert_into_map(self, "avg",          .Avg)
	_insert_into_map(self, "bigint",       .Bigint)
	_insert_into_map(self, "cast",         .Cast)
	_insert_into_map(self, "try_cast",     .Try_Cast)
	_insert_into_map(self, "checksum",     .Checksum)
	_insert_into_map(self, "checksum_agg", .Checksum_Agg)
	_insert_into_map(self, "concat",       .Concat)
	_insert_into_map(self, "count",        .Count)
	_insert_into_map(self, "dateadd",      .Dateadd)
	_insert_into_map(self, "datediff",     .Datediff)
	_insert_into_map(self, "datename",     .Datename)
	_insert_into_map(self, "datepart",     .Datepart)
	_insert_into_map(self, "days",         .Days)
	_insert_into_map(self, "dense_rank",   .Dense_Rank)
	_insert_into_map(self, "getdate",      .Getdate)
	_insert_into_map(self, "getutcdate",   .Getutcdate)
	_insert_into_map(self, "go",           .Go)
	_insert_into_map(self, "hash",         .Hash)
	_insert_into_map(self, "hours",        .Hours)
	_insert_into_map(self, "int",          .Int)
	_insert_into_map(self, "max",          .Max)
	_insert_into_map(self, "min",          .Min)
	_insert_into_map(self, "minutes",      .Minutes)
	_insert_into_map(self, "range",        .Range)
	_insert_into_map(self, "rank",         .Rank)
	_insert_into_map(self, "row",          .Row)
	_insert_into_map(self, "row_number",   .Row_Number)
	_insert_into_map(self, "rows",         .Rows)
	_insert_into_map(self, "seconds",      .Seconds)
	_insert_into_map(self, "smallint",     .Smallint)
	_insert_into_map(self, "static",       .Static)
	_insert_into_map(self, "statusonly",   .Statusonly)
	_insert_into_map(self, "stdev",        .Stdev)
	_insert_into_map(self, "stdevp",       .Stdevp)
	_insert_into_map(self, "string_agg",   .String_Agg)
	_insert_into_map(self, "stuff",        .Stuff)
	_insert_into_map(self, "sum",          .Sum)
	_insert_into_map(self, "tinyint",      .Tinyint)
	_insert_into_map(self, "wait",         .Wait)
	_insert_into_map(self, "waitfor",      .Waitfor)
	_insert_into_map(self, "isnull",       .Isnull)
	_insert_into_map(self, "varchar",      .Varchar)
	_insert_into_map(self, "nvarchar",     .Nvarchar)

	_insert_into_map(self, "#",  .Sym_Pound)
	_insert_into_map(self, "(",  .Sym_Lparen)
	_insert_into_map(self, ")",  .Sym_Rparen)
	_insert_into_map(self, "+=", .Sym_Plus_Assign)
	_insert_into_map(self, "-=", .Sym_Minus_Assign)
	_insert_into_map(self, "*=", .Sym_Multiply_Assign)
	_insert_into_map(self, "/=", .Sym_Divide_Assign)
	_insert_into_map(self, "%=", .Sym_Modulus_Assign)
	_insert_into_map(self, "~=", .Sym_Bit_Not_Assign)
	_insert_into_map(self, "|=", .Sym_Bit_Or_Assign)
	_insert_into_map(self, "&=", .Sym_Bit_And_Assign)
	_insert_into_map(self, "^=", .Sym_Bit_Xor_Assign)
	_insert_into_map(self, "+",  .Sym_Plus)
	_insert_into_map(self, "-",  .Sym_Minus)
	_insert_into_map(self, "*",  .Sym_Multiply)
	_insert_into_map(self, "/",  .Sym_Divide)
	_insert_into_map(self, "%",  .Sym_Modulus)
	_insert_into_map(self, "~",  .Sym_Bit_Not_Unary)
	_insert_into_map(self, "|",  .Sym_Bit_Or)
	_insert_into_map(self, "&",  .Sym_Bit_And)
	_insert_into_map(self, "^",  .Sym_Bit_Xor)
	_insert_into_map(self, ".",  .Sym_Dot)
	_insert_into_map(self, "=",  .Sym_Eq)
	_insert_into_map(self, "!=", .Sym_Ne)
	_insert_into_map(self, "<>", .Sym_Ne)
	_insert_into_map(self, ">",  .Sym_Gt)
	_insert_into_map(self, ">=", .Sym_Ge)
	_insert_into_map(self, "<",  .Sym_Lt)
	_insert_into_map(self, "<=", .Sym_Le)
	_insert_into_map(self, ",",  .Sym_Comma)
	_insert_into_map(self, ";",  .Sym_Semicolon)
	_insert_into_map(self, "/*", .Sym_Block_Comment)
	_insert_into_map(self, "--", .Sym_Line_Comment)

	//fmt.fprintf(os.stderr, "mapsize: %d\n", len(self.tok_map))
}

@(private="file")
_skip_whitespace :: proc(self: ^Parser, idx: ^u32)
{
	for ; idx^ < u32(len(self.q)) && unicode.is_space(rune(self.q[idx^])); idx^ += 1 {
		if self.q[idx^] == '\n' {
			append(&self.lf_vec, idx^)
		}
	}
}

@(private="file")
_get_name :: proc(self: ^Parser, group: int, idx: ^u32) {
	begin := idx^
	for ; idx^ < u32(len(self.q)) && (self.q[idx^] == '_' ||
	      unicode.is_digit(rune(self.q[idx^])) ||
	      unicode.is_alpha(rune(self.q[idx^]))); idx^ += 1 {}

	type, ok := self.tok_map[self.q[begin:idx^]]
	if !ok {
		type = .Query_Name
	}

	append(&self.tokens, Token {
		    type=type,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })
}

@(private="file")
_get_qualified_name :: proc(self: ^Parser, group: int, idx: ^u32) -> Result {
	real_begin := idx^ + 1
	for ; idx^ < u32(len(self.q)) && self.q[idx^] != ']'; idx^ += 1 {}

	if idx^ >= u32(len(self.q)) {
		return lex_error(self, idx^, "unmatched '['")
	}

	idx^ += 1

	real_end := idx^ - 1
	if real_begin == idx^ {
		real_end = real_begin
	}

	append(&self.tokens, Token {
		    type = .Query_Name,
		    group=u16(group),
		    begin = real_begin,
		    len = real_end-real_begin })

	return .Ok
}

@(private="file")
_get_numeric :: proc(self: ^Parser, group: int, idx: ^u32) -> Result {
	/* TODO hex check here ? */

	begin := idx^
	is_float: bool

	for ; idx^ < u32(len(self.q)) &&
	    (unicode.is_digit(rune(self.q[idx^])) || self.q[idx^] == '.'); idx^ += 1 {
		if self.q[idx^] == '.' {
			if is_float {
				return lex_error(self, idx^, "malformed decimal")
			}
			is_float = true
		}
	}

	type := Token_Type.Literal_Int
	if is_float {
		type = .Literal_Float
	}

	append(&self.tokens, Token {
		    type=type,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private="file")
_get_variable :: proc(self: ^Parser, group: int, idx: ^u32) {
	begin := idx^
	idx^ += 1

	for ; self.q[idx^] == '_' ||
	      unicode.is_digit(rune(self.q[idx^])) ||
	      unicode.is_alpha(rune(self.q[idx^])); idx^ += 1 {
	}

	append(&self.tokens, Token {
		    type=.Query_Variable,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })
}

@(private="file")
_get_block_comment :: proc(self: ^Parser, group: int, idx: ^u32) -> Result {
	
	begin := idx^

	for ; idx^+1 < u32(len(self.q)) && 
	    !(self.q[idx^] == '*' && self.q[idx^+1] == '/'); idx^ += 1 {
		if self.q[idx^] == '\n' {
			append(&self.lf_vec, idx^)
		}
	}

	if idx^+1 >= u32(len(self.q)) {
		return lex_error(self, idx^, "unmatched `/*'")
	}

	idx^ += 2

	append(&self.tokens, Token {
		    type=.Query_Comment,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private="file")
_get_line_comment :: proc(self: ^Parser, group: int, idx: ^u32) -> Result {
	begin := idx^
	offset := strings.index_byte(self.q[idx^:], '\n')
	if offset == -1 {
		idx^ = u32(len(self.q))
	} else {
		idx^ += u32(offset)
		append(&self.lf_vec, idx^)
		idx^ += 1
	}

	append(&self.tokens, Token {
		    type=.Query_Comment,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private="file")
_get_symbol :: proc(self: ^Parser, group: int, idx: ^u32) -> Result {
	begin := idx^
	//idx^ += 1

	type : Token_Type
	ok : bool

	/* Check for 2 character symbols first */
	if begin < u32(len(self.q)) {
		type, ok = self.tok_map[self.q[begin:begin+2]]
	}

	if ok {
		#partial switch type {
		case .Sym_Line_Comment:
			return _get_line_comment(self, group, idx)
		case .Sym_Block_Comment:
			return _get_block_comment(self, group, idx)
		}
	}

	if ok {
		idx^ += 2
	} else {
		idx^ += 1
		type, ok = self.tok_map[self.q[begin:idx^]]
	}

	if !ok {
		return lex_error(self, idx^, "invalid symbol")
	}

	append(&self.tokens, Token {
		    type=type,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private = "file")
_is_symbol :: proc(c: u8) -> bool {
	return strings.index_byte("#()!=+-*/%~|&^.<>,;", c) >= 0
}

@(private = "file")
_lex_tokenize :: proc(self: ^Parser) -> Result {
	i : u32 = 0
	append(&self.tokens, Token { type=.Query_Begin })

	group : int
	group_stack : [dynamic]int
	append(&group_stack, group)
	defer delete(group_stack)

	ret := 0

	for ret == 0 && i < u32(len(self.q)) {
		tok_len := 0
		switch {
		case unicode.is_space(rune(self.q[i])):
			_skip_whitespace(self, &i)
		case self.q[i] == '[':
			_get_qualified_name(self, group, &i) or_return
		case unicode.is_digit(rune(self.q[i])) ||
		    (i+1 < u32(len(self.q)) && unicode.is_digit(rune(self.q[i]))):
			_get_numeric(self, group, &i) or_return
		case self.q[i] == '@':
			_get_variable(self, group, &i)
		case self.q[i] == '(':
			group += 1
			bit_array.set(&self.consumed, len(self.tokens))
			append(&group_stack, group)
			append(&self.tokens, Token {type=.Sym_Lparen, group=u16(group), begin=i, len=1})
			i += 1
		case self.q[i] == ')':
			if len(group_stack) == 1 {
				return lex_error(self, i, "unmatched ')'")
			}
			bit_array.set(&self.consumed, len(self.tokens))
			append(&self.tokens, Token {type=.Sym_Rparen, group=u16(group), begin=i, len=1})
			i += 1
			pop(&group_stack)
			group = group_stack[len(group_stack)-1]
		case _is_symbol(self.q[i]):
			_get_symbol(self, group, &i) or_return
		case self.q[i] == '_' ||
		    unicode.is_digit(rune(self.q[i])) ||
		    unicode.is_alpha(rune(self.q[i])):
			_get_name(self, group, &i)
		case:
			return lex_error(self, i)
		}
	}

	if len(group_stack) > 1 {
		return lex_error(self, i, "unmatched '('")
	}

	append(&self.tokens, Token { type = .Query_End })

	/* Dump tokens */
	//for tok in self.tokens {
	//	if enum_name, ok := fmt.enum_value_to_string(tok.type); ok {
	//		if tok.len > 0 {
	//			fmt.println(enum_name, self.q[tok.begin:tok.begin+tok.len])
	//		} else {
	//			fmt.println(enum_name)
	//		}
	//	}
	//}

	return .Ok
}

@(test)
lex_error_check :: proc(t: ^testing.T) {
	parser := make_parser()

	/* Unmatched tokens */
	parser.q = "select a,b,c,[ntll from foo where 1=1"
	testing.expect_value(t, lex_lex(&parser), Result.Error)

	parser.q = "select a,b,c,ntll] from foo where 1=1"
	testing.expect_value(t, lex_lex(&parser), Result.Error)

	parser.q = "select 124+35*24 / (124-2 from [foo] where 1<>2"
	testing.expect_value(t, lex_lex(&parser), Result.Error)

	parser.q = "select 124+35*24 / (124-2))"
	testing.expect_value(t, lex_lex(&parser), Result.Error)

	parser.q = "select /* a comment * / 1,2 from foo"
	testing.expect_value(t, lex_lex(&parser), Result.Error)
	
	/* Will throw parser error as multiply, divide */
	//parser.q = "select / * a comment */ 1,2 from foo"
	//testing.expect_value(t, lex_lex(&parser), Result.Error)

	/* Illegal symbols */
	parser.q = "select $var from foo join foo on 1=1"
	testing.expect_value(t, lex_lex(&parser), Result.Error)

	parser.q = "select `col` from foo join foo on 1=1"
	testing.expect_value(t, lex_lex(&parser), Result.Error)

	/* Malformed number */
	parser.q = "select 1234.1234.1234 from foo join foo on 1=1"
	testing.expect_value(t, lex_lex(&parser), Result.Error)

	/* Oh shit this is actually legal in SQL Server... */
	//parser.q = "select 1234shnt from foo join foo on 1=1"
	//testing.expect_value(t, lex_lex(&parser), Result.Error)

	parse_destroy(&parser)
}

@(test)
lex_check :: proc(t: ^testing.T) {
	parser := make_parser()

	/* For the following tests...
	 * len(parser.tokens) = token_count + 2
	 * This is because every query begins and ends 
	 * with .Query_Begin and .Query_End
	 */
	
	//         01      2   3    4   5    6   7  890 1
	parser.q = "select col from foo join foo on 1=1"
	testing.expect_value(t, lex_lex(&parser), Result.Ok)
	testing.expect_value(t, len(parser.tokens), 12)
	testing.expect_value(t, parser.tokens[0].type, Token_Type.Query_Begin)
	testing.expect_value(t, parser.tokens[1].type, Token_Type.Select)
	testing.expect_value(t, parser.tokens[2].type, Token_Type.Query_Name)
	testing.expect_value(t, parser.tokens[3].type, Token_Type.From)
	testing.expect_value(t, parser.tokens[4].type, Token_Type.Query_Name)
	testing.expect_value(t, parser.tokens[5].type, Token_Type.Join)
	testing.expect_value(t, parser.tokens[6].type, Token_Type.Query_Name)
	testing.expect_value(t, parser.tokens[7].type, Token_Type.On)
	testing.expect_value(t, parser.tokens[8].type, Token_Type.Literal_Int)
	testing.expect_value(t, parser.tokens[9].type, Token_Type.Sym_Eq)
	testing.expect_value(t, parser.tokens[10].type, Token_Type.Literal_Int)
	testing.expect_value(t, parser.tokens[11].type, Token_Type.Query_End)

	//0      1
	//      2      34 5 6 7 8 9 01 234567 8

	//      9    0   1
	//23
	//4  56      78 9
	parser.q = `
	select (
		select (1 + 2 * 5 + (33-(1))) [bar baz]
		from foo f
	) f 
	from (select 1) x
	`
	testing.expect_value(t, lex_lex(&parser), Result.Ok)
	testing.expect_value(t, len(parser.tokens), 32)

	parse_destroy(&parser)
}
