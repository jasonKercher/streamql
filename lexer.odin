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

lex_lex :: proc (parser: ^Parser) -> Result {
	if len(parser.tok_map) == 0 {
		_init_map(parser)
	}

	resize(&parser.tokens, 0)
	resize(&parser.lf_vec, 0)
	bit_array.clear(&parser.consumed)

	return _lex_tokenize(parser)
}

lex_error :: proc(parser: ^Parser, idx: u32, msg: string = "lex error") -> Result {
	line, off := parse_get_pos(parser, idx)
	fmt.fprintf(os.stderr, "%s (line: %d, pos: %d)\n", msg, line, off)
	return .Error
}

@(private="file")
_insert_into_map :: proc(parser: ^Parser, key: string, type: Token_Type) {
	parser.tok_map[key] = type
}

@(private="file")
_init_map :: proc(parser: ^Parser) {
	_insert_into_map(parser, "abs",          .Abs)
	_insert_into_map(parser, "ascii",        .Ascii)
	_insert_into_map(parser, "ceiling",      .Ceiling)
	_insert_into_map(parser, "char",         .Char)
	_insert_into_map(parser, "charindex",    .Charindex)
	_insert_into_map(parser, "datalength",   .Datalength)
	_insert_into_map(parser, "day",          .Day)
	_insert_into_map(parser, "floor",        .Floor)
	_insert_into_map(parser, "isdate",       .Isdate)
	_insert_into_map(parser, "isnumeric",    .Isnumeric)
	_insert_into_map(parser, "len",          .Len)
	_insert_into_map(parser, "lower",        .Lower)
	_insert_into_map(parser, "ltrim",        .Ltrim)
	_insert_into_map(parser, "month",        .Month)
	_insert_into_map(parser, "nchar",        .Nchar)
	_insert_into_map(parser, "patindex",     .Patindex)
	_insert_into_map(parser, "rand",         .Rand)
	_insert_into_map(parser, "replace",      .Replace)
	_insert_into_map(parser, "round",        .Round)
	_insert_into_map(parser, "rtrim",        .Rtrim)
	_insert_into_map(parser, "sign",         .Sign)
	_insert_into_map(parser, "space",        .Space)
	_insert_into_map(parser, "str",          .Str)
	_insert_into_map(parser, "substring",    .Substring)
	_insert_into_map(parser, "upper",        .Upper)
	_insert_into_map(parser, "user_name",    .User_Name)
	_insert_into_map(parser, "year",         .Year)
	_insert_into_map(parser, "text",         .Text)
	_insert_into_map(parser, "ntext",        .Ntext)
	_insert_into_map(parser, "add",          .Add)
	_insert_into_map(parser, "all",          .All)
	_insert_into_map(parser, "alter",        .Alter)
	_insert_into_map(parser, "and",          .And)
	_insert_into_map(parser, "any",          .Any)
	_insert_into_map(parser, "as",           .As)
	_insert_into_map(parser, "begin",        .Begin)
	_insert_into_map(parser, "between",      .Between)
	_insert_into_map(parser, "break",        .Break)
	_insert_into_map(parser, "by",           .By)
	_insert_into_map(parser, "case",         .Case)
	_insert_into_map(parser, "coalesce",     .Coalesce)
	_insert_into_map(parser, "column",       .Column)
	_insert_into_map(parser, "continue",     .Continue)
	_insert_into_map(parser, "convert",      .Convert)
	_insert_into_map(parser, "create",       .Create)
	_insert_into_map(parser, "cross",        .Cross)
	_insert_into_map(parser, "declare",      .Declare)
	_insert_into_map(parser, "delete",       .Delete)
	_insert_into_map(parser, "desc",         .Desc)
	_insert_into_map(parser, "distinct",     .Distinct)
	_insert_into_map(parser, "distributed",  .Distributed)
	_insert_into_map(parser, "drop",         .Drop)
	_insert_into_map(parser, "else",         .Else)
	_insert_into_map(parser, "end",          .End)
	_insert_into_map(parser, "execute",      .Execute)
	_insert_into_map(parser, "exists",       .Exists)
	_insert_into_map(parser, "from",         .From)
	_insert_into_map(parser, "full",         .Full)
	_insert_into_map(parser, "function",     .Function)
	_insert_into_map(parser, "goto",         .Goto)
	_insert_into_map(parser, "group",        .Group)
	_insert_into_map(parser, "having",       .Having)
	_insert_into_map(parser, "if",           .If)
	_insert_into_map(parser, "in",           .In)
	_insert_into_map(parser, "inner",        .Inner)
	_insert_into_map(parser, "insert",       .Insert)
	_insert_into_map(parser, "into",         .Into)
	_insert_into_map(parser, "is",           .Is)
	_insert_into_map(parser, "join",         .Join)
	_insert_into_map(parser, "left",         .Left)
	_insert_into_map(parser, "like",         .Like)
	_insert_into_map(parser, "not",          .Not)
	_insert_into_map(parser, "null",         .Null)
	_insert_into_map(parser, "nullif",       .Nullif)
	_insert_into_map(parser, "of",           .Of)
	_insert_into_map(parser, "off",          .Off)
	_insert_into_map(parser, "on",           .On)
	_insert_into_map(parser, "open",         .Open)
	_insert_into_map(parser, "or",           .Or)
	_insert_into_map(parser, "order",        .Order)
	_insert_into_map(parser, "over",         .Over)
	_insert_into_map(parser, "percent",      .Percent)
	_insert_into_map(parser, "print",        .Print)
	_insert_into_map(parser, "proc",         .Proc)
	_insert_into_map(parser, "procedure",    .Procedure)
	_insert_into_map(parser, "raiserror",    .Raiserror)
	_insert_into_map(parser, "replication",  .Replication)
	_insert_into_map(parser, "return",       .Return)
	_insert_into_map(parser, "revert",       .Revert)
	_insert_into_map(parser, "right",        .Right)
	_insert_into_map(parser, "rollback",     .Rollback)
	_insert_into_map(parser, "save",         .Save)
	_insert_into_map(parser, "schema",       .Schema)
	_insert_into_map(parser, "select",       .Select)
	_insert_into_map(parser, "set",          .Set)
	_insert_into_map(parser, "table",        .Table)
	_insert_into_map(parser, "then",         .Then)
	_insert_into_map(parser, "to",           .To)
	_insert_into_map(parser, "top",          .Top)
	_insert_into_map(parser, "tran",         .Tran)
	_insert_into_map(parser, "transaction",  .Transaction)
	_insert_into_map(parser, "truncate",     .Truncate)
	_insert_into_map(parser, "union",        .Union)
	_insert_into_map(parser, "update",       .Update)
	_insert_into_map(parser, "user",         .User)
	_insert_into_map(parser, "values",       .Values)
	_insert_into_map(parser, "when",         .When)
	_insert_into_map(parser, "where",        .Where)
	_insert_into_map(parser, "while",        .While)
	_insert_into_map(parser, "avg",          .Avg)
	_insert_into_map(parser, "bigint",       .Bigint)
	_insert_into_map(parser, "cast",         .Cast)
	_insert_into_map(parser, "try_cast",     .Try_Cast)
	_insert_into_map(parser, "checksum",     .Checksum)
	_insert_into_map(parser, "checksum_agg", .Checksum_Agg)
	_insert_into_map(parser, "concat",       .Concat)
	_insert_into_map(parser, "count",        .Count)
	_insert_into_map(parser, "dateadd",      .Dateadd)
	_insert_into_map(parser, "datediff",     .Datediff)
	_insert_into_map(parser, "datename",     .Datename)
	_insert_into_map(parser, "datepart",     .Datepart)
	_insert_into_map(parser, "days",         .Days)
	_insert_into_map(parser, "dense_rank",   .Dense_Rank)
	_insert_into_map(parser, "getdate",      .Getdate)
	_insert_into_map(parser, "getutcdate",   .Getutcdate)
	_insert_into_map(parser, "go",           .Go)
	_insert_into_map(parser, "hash",         .Hash)
	_insert_into_map(parser, "hours",        .Hours)
	_insert_into_map(parser, "int",          .Int)
	_insert_into_map(parser, "max",          .Max)
	_insert_into_map(parser, "min",          .Min)
	_insert_into_map(parser, "minutes",      .Minutes)
	_insert_into_map(parser, "range",        .Range)
	_insert_into_map(parser, "rank",         .Rank)
	_insert_into_map(parser, "row",          .Row)
	_insert_into_map(parser, "row_number",   .Row_Number)
	_insert_into_map(parser, "rows",         .Rows)
	_insert_into_map(parser, "seconds",      .Seconds)
	_insert_into_map(parser, "smallint",     .Smallint)
	_insert_into_map(parser, "static",       .Static)
	_insert_into_map(parser, "statusonly",   .Statusonly)
	_insert_into_map(parser, "stdev",        .Stdev)
	_insert_into_map(parser, "stdevp",       .Stdevp)
	_insert_into_map(parser, "string_agg",   .String_Agg)
	_insert_into_map(parser, "stuff",        .Stuff)
	_insert_into_map(parser, "sum",          .Sum)
	_insert_into_map(parser, "tinyint",      .Tinyint)
	_insert_into_map(parser, "wait",         .Wait)
	_insert_into_map(parser, "waitfor",      .Waitfor)
	_insert_into_map(parser, "isnull",       .Isnull)
	_insert_into_map(parser, "varchar",      .Varchar)
	_insert_into_map(parser, "nvarchar",     .Nvarchar)

	_insert_into_map(parser, "#",  .Sym_Pound)
	_insert_into_map(parser, "(",  .Sym_Lparen)
	_insert_into_map(parser, ")",  .Sym_Rparen)
	_insert_into_map(parser, "+=", .Sym_Plus_Assign)
	_insert_into_map(parser, "-=", .Sym_Minus_Assign)
	_insert_into_map(parser, "*=", .Sym_Multiply_Assign)
	_insert_into_map(parser, "/=", .Sym_Divide_Assign)
	_insert_into_map(parser, "%=", .Sym_Modulus_Assign)
	_insert_into_map(parser, "~=", .Sym_Bit_Not_Assign)
	_insert_into_map(parser, "|=", .Sym_Bit_Or_Assign)
	_insert_into_map(parser, "&=", .Sym_Bit_And_Assign)
	_insert_into_map(parser, "^=", .Sym_Bit_Xor_Assign)
	_insert_into_map(parser, "+",  .Sym_Plus)
	_insert_into_map(parser, "-",  .Sym_Minus)
	_insert_into_map(parser, "*",  .Sym_Multiply)
	_insert_into_map(parser, "/",  .Sym_Divide)
	_insert_into_map(parser, "%",  .Sym_Modulus)
	_insert_into_map(parser, "~",  .Sym_Bit_Not_Unary)
	_insert_into_map(parser, "|",  .Sym_Bit_Or)
	_insert_into_map(parser, "&",  .Sym_Bit_And)
	_insert_into_map(parser, "^",  .Sym_Bit_Xor)
	_insert_into_map(parser, ".",  .Sym_Dot)
	_insert_into_map(parser, "=",  .Sym_Eq)
	_insert_into_map(parser, "!=", .Sym_Ne)
	_insert_into_map(parser, "<>", .Sym_Ne)
	_insert_into_map(parser, ">",  .Sym_Gt)
	_insert_into_map(parser, ">=", .Sym_Ge)
	_insert_into_map(parser, "<",  .Sym_Lt)
	_insert_into_map(parser, "<=", .Sym_Le)
	_insert_into_map(parser, ",",  .Sym_Comma)
	_insert_into_map(parser, ";",  .Sym_Semicolon)
	_insert_into_map(parser, "/*", .Sym_Block_Comment)
	_insert_into_map(parser, "--", .Sym_Line_Comment)

	//fmt.fprintf(os.stderr, "mapsize: %d\n", len(parser.tok_map))
}

@(private="file")
_skip_whitespace :: proc(parser: ^Parser, idx: ^u32)
{
	for ; idx^ < u32(len(parser.q)) && unicode.is_space(rune(parser.q[idx^])); idx^ += 1 {
		if parser.q[idx^] == '\n' {
			append(&parser.lf_vec, idx^)
		}
	}
}

@(private="file")
_get_name :: proc(parser: ^Parser, group: int, idx: ^u32) {
	begin := idx^
	for ; idx^ < u32(len(parser.q)) && (parser.q[idx^] == '_' ||
	      unicode.is_digit(rune(parser.q[idx^])) ||
	      unicode.is_alpha(rune(parser.q[idx^]))); idx^ += 1 {}

	type, ok := parser.tok_map[parser.q[begin:idx^]]
	if !ok {
		type = .Query_Name
	}

	append(&parser.tokens, Token {
		    type=type,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })
}

@(private="file")
_get_string :: proc(parser: ^Parser, group: int, idx: ^u32) -> Result {
	idx^ += 1
	real_begin := idx^
	for ; idx^ < u32(len(parser.q)) && parser.q[idx^] != '\''; idx^ += 1 {}

	if idx^ >= u32(len(parser.q)) {
		return lex_error(parser, idx^, "unmatched '\''")
	}

	idx^ += 1

	real_end := idx^ - 1
	if real_begin == idx^ {
		real_end = real_begin
	}

	append(&parser.tokens, Token {
		    type = .Literal_String,
		    group=u16(group),
		    begin = real_begin,
		    len = real_end-real_begin })

	return .Ok
}

@(private="file")
_get_qualified_name :: proc(parser: ^Parser, group: int, idx: ^u32) -> Result {
	idx^ += 1
	real_begin := idx^
	for ; idx^ < u32(len(parser.q)) && parser.q[idx^] != ']'; idx^ += 1 {}

	if idx^ >= u32(len(parser.q)) {
		return lex_error(parser, idx^, "unmatched '['")
	}

	idx^ += 1

	real_end := idx^ - 1
	if real_begin == idx^ {
		real_end = real_begin
	}

	append(&parser.tokens, Token {
		    type = .Query_Name,
		    group=u16(group),
		    begin = real_begin,
		    len = real_end-real_begin })

	return .Ok
}

@(private="file")
_get_numeric :: proc(parser: ^Parser, group: int, idx: ^u32) -> Result {
	/* TODO hex check here ? */

	begin := idx^
	is_float: bool

	for ; idx^ < u32(len(parser.q)) &&
	    (unicode.is_digit(rune(parser.q[idx^])) || parser.q[idx^] == '.'); idx^ += 1 {
		if parser.q[idx^] == '.' {
			if is_float {
				return lex_error(parser, idx^, "malformed decimal")
			}
			is_float = true
		}
	}

	type := Token_Type.Literal_Int
	if is_float {
		type = .Literal_Float
	}

	append(&parser.tokens, Token {
		    type=type,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private="file")
_get_variable :: proc(parser: ^Parser, group: int, idx: ^u32) {
	begin := idx^
	idx^ += 1

	for ; parser.q[idx^] == '_' ||
	      unicode.is_digit(rune(parser.q[idx^])) ||
	      unicode.is_alpha(rune(parser.q[idx^])); idx^ += 1 {
	}

	append(&parser.tokens, Token {
		    type=.Query_Variable,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })
}

@(private="file")
_get_block_comment :: proc(parser: ^Parser, group: int, idx: ^u32) -> Result {
	
	begin := idx^

	for ; idx^+1 < u32(len(parser.q)) && 
	    !(parser.q[idx^] == '*' && parser.q[idx^+1] == '/'); idx^ += 1 {
		if parser.q[idx^] == '\n' {
			append(&parser.lf_vec, idx^)
		}
	}

	if idx^+1 >= u32(len(parser.q)) {
		return lex_error(parser, idx^, "unmatched `/*'")
	}

	idx^ += 2

	append(&parser.tokens, Token {
		    type=.Query_Comment,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private="file")
_get_line_comment :: proc(parser: ^Parser, group: int, idx: ^u32) -> Result {
	begin := idx^
	offset := strings.index_byte(parser.q[idx^:], '\n')
	if offset == -1 {
		idx^ = u32(len(parser.q))
	} else {
		idx^ += u32(offset)
		append(&parser.lf_vec, idx^)
		idx^ += 1
	}

	append(&parser.tokens, Token {
		    type=.Query_Comment,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private="file")
_get_symbol :: proc(parser: ^Parser, group: int, idx: ^u32) -> Result {
	begin := idx^
	//idx^ += 1

	type : Token_Type
	ok : bool

	/* Check for 2 character symbols first */
	if begin < u32(len(parser.q)) {
		type, ok = parser.tok_map[parser.q[begin:begin+2]]
	}

	if ok {
		#partial switch type {
		case .Sym_Line_Comment:
			return _get_line_comment(parser, group, idx)
		case .Sym_Block_Comment:
			return _get_block_comment(parser, group, idx)
		}
	}

	if ok {
		idx^ += 2
	} else {
		idx^ += 1
		type, ok = parser.tok_map[parser.q[begin:idx^]]
	}

	if !ok {
		return lex_error(parser, idx^, "invalid symbol")
	}

	append(&parser.tokens, Token {
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
_lex_tokenize :: proc(parser: ^Parser) -> Result {
	i : u32 = 0
	append(&parser.tokens, Token { type=.Query_Begin })

	group : int
	group_stack : [dynamic]int
	append(&group_stack, group)
	defer delete(group_stack)

	ret := 0

	for ret == 0 && i < u32(len(parser.q)) {
		tok_len := 0
		switch {
		case unicode.is_space(rune(parser.q[i])):
			_skip_whitespace(parser, &i)
		case parser.q[i] == '\'':
			_get_string(parser, group, &i)
		case parser.q[i] == '[':
			_get_qualified_name(parser, group, &i) or_return
		case unicode.is_digit(rune(parser.q[i])) ||
		    (i+1 < u32(len(parser.q)) && unicode.is_digit(rune(parser.q[i]))):
			_get_numeric(parser, group, &i) or_return
		case parser.q[i] == '@':
			_get_variable(parser, group, &i)
		case parser.q[i] == '(':
			group += 1
			bit_array.set(&parser.consumed, len(parser.tokens))
			append(&group_stack, group)
			append(&parser.tokens, Token {type=.Sym_Lparen, group=u16(group), begin=i, len=1})
			i += 1
		case parser.q[i] == ')':
			if len(group_stack) == 1 {
				return lex_error(parser, i, "unmatched ')'")
			}
			bit_array.set(&parser.consumed, len(parser.tokens))
			append(&parser.tokens, Token {type=.Sym_Rparen, group=u16(group), begin=i, len=1})
			i += 1
			pop(&group_stack)
			group = group_stack[len(group_stack)-1]
		case _is_symbol(parser.q[i]):
			_get_symbol(parser, group, &i) or_return
		case parser.q[i] == '_' ||
		    unicode.is_digit(rune(parser.q[i])) ||
		    unicode.is_alpha(rune(parser.q[i])):
			_get_name(parser, group, &i)
		case:
			return lex_error(parser, i)
		}
	}

	if len(group_stack) > 1 {
		return lex_error(parser, i, "unmatched '('")
	}

	append(&parser.tokens, Token { type = .Query_End })

	/* Dump tokens */
	//for tok in parser.tokens {
	//	if enum_name, ok := fmt.enum_value_to_string(tok.type); ok {
	//		if tok.len > 0 {
	//			fmt.println(enum_name, parser.q[tok.begin:tok.begin+tok.len])
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

	destroy_parser(&parser)
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

	destroy_parser(&parser)
}

