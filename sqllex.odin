package streamql

import "core:os"
import "core:fmt"
import "core:strings"
import "core:unicode"

Token :: struct {
	begin:  u32,
	len:    u32,
	grp:    u32,
	mingrp: u32,  /* Intending to get rid of this */
	type:   Token_Type,
}

Token_Type :: enum {
	Query_Begin,    /* All parsing begins here... */
	Query_End,      /*    ... and ends here       */
	Query_Name,
	Query_Variable,
	Literal_Int,
	Literal_Float,
	Literal_String,

	/* Keywords */
	Abs,
	Ascii,
	Ceiling,
	Char,
	Charindex,
	Datalength,
	Day,
	Floor,
	Isdate,
	Isnumeric,
	Len,
	Lower,
	Ltrim,
	Month,
	Nchar,
	Patindex,
	Rand,
	Replace,
	Round,
	Rtrim,
	Sign,
	Space,
	Str,
	Substring,
	Upper,
	User_Name,
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
	Coalesce,
	Column,
	Continue,
	Convert,
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
	Left,
	Like,
	Not,
	Null,
	Nullif,
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
	Right,
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
	Avg,
	Bigint,
	Cast,
	Try_Cast,
	Checksum,
	Checksum_Agg,
	Concat,
	Count,
	Dateadd,
	Datediff,
	Datename,
	Datepart,
	Days,
	Dense_Rank,
	Getdate,
	Getutcdate,
	Go,
	Hash,
	Hours,
	Int,
	Max,
	Min,
	Minutes,
	Range,
	Rank,
	Row,
	Row_Number,
	Rows,
	Seconds,
	Smallint,
	Static,
	Statusonly,
	Stdev,
	Stdevp,
	String_Agg,
	Stuff,
	Sum,
	Tinyint,
	Wait,
	Waitfor,
	Isnull,
	Varchar,
	Nvarchar,

	/* symbols */
	Sym_Pound,
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

	/* Special un-mapped tokens */
	End_Of_Subquery, /* was Sym_Rparen */
	Sym_Asterisk,    /* was Sym_Multiply */
	Sym_Plus_Unary,  /* was Sym_Plus */
	Sym_Minus_Unary, /* was Sym_Minus */
}

@(private)
_insert_into_map :: proc(self: ^Sql_Parser, key: string, type: Token_Type) {
	self.tok_map[key] = type
}

@(private)
_init_map :: proc(self: ^Sql_Parser) {
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
}

lex_lex :: proc (self: ^Sql_Parser) {
	if len(self.tok_map) == 0 {
		_init_map(self)
	}

	lex_tokenize(self)

}

lex_error :: proc(self: ^Sql_Parser, idx: u32) -> Sql_Return {
	return .Error
}

@(private)
_skip_whitespace :: proc(self: ^Sql_Parser, idx: ^u32)
{
	for ; idx^ < cast(u32)len(self.q) && unicode.is_space(cast(rune)self.q[idx^]); idx^ += 1 {
		if self.q[idx^] == '\n' {
			append(&self.lf_vec, idx^)
		}
	}
}

@(private)
_get_name :: proc(self: ^Sql_Parser, group: int, idx: ^u32) {
	begin := idx^
	for ; self.q[idx^] == '_' ||
	      unicode.is_digit(cast(rune)self.q[idx^]) ||
	      unicode.is_alpha(cast(rune)self.q[idx^]); idx^ += 1 {}

	type, ok := self.tok_map[self.q[begin:idx^]]
	if !ok {
		type = .Query_Name
	}

	append(&self.tok_vec, Token {
		    type=type,
		    grp=cast(u32)group,
		    begin=begin,
		    len=idx^-begin })
}

@(private)
_get_qualified_name :: proc(self: ^Sql_Parser, group: int, idx: ^u32) -> Sql_Return {
	real_begin := idx^ + 1
	for ; self.q[idx^] != ']' && idx^ < cast(u32)len(self.q); idx^ += 1 {}

	if self.q[idx^] != ']' {
		return lex_error(self, idx^)
	}

	idx^ += 1

	real_end := idx^ - 1
	if real_begin == idx^ {
		real_end = real_begin
	}

	append(&self.tok_vec, Token {
		    type = .Query_Name,
		    grp=cast(u32)group,
		    begin = real_begin,
		    len = real_end-real_begin })

	return .Ok
}

@(private)
_get_numeric :: proc(self: ^Sql_Parser, group: int, idx: ^u32) -> Sql_Return {
	/* TODO hex check here ? */

	begin := idx^
	is_float: bool

	for ; idx^ < cast(u32)len(self.q) &&
	    (unicode.is_digit(cast(rune)self.q[idx^]) || self.q[idx^] == '.'); idx^ += 1 {
		if self.q[idx^] == '.' {
			if is_float {
				return lex_error(self, idx^)
			}
			is_float = true
		}
	}

	type := Token_Type.Literal_Int
	if is_float {
		type = .Literal_Float
	}

	append(&self.tok_vec, Token {
		    type=type,
		    grp=cast(u32)group,
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private)
_get_variable :: proc(self: ^Sql_Parser, group: int, idx: ^u32) {
	begin := idx^
	idx^ += 1

	for ; self.q[idx^] == '_' ||
	      unicode.is_digit(cast(rune)self.q[idx^]) ||
	      unicode.is_alpha(cast(rune)self.q[idx^]); idx^ += 1 {
	}

	append(&self.tok_vec, Token {
		    type=.Query_Variable,
		    grp=cast(u32)group,
		    begin=begin,
		    len=idx^-begin })
}

@(private)
_get_block_comment :: proc(self: ^Sql_Parser, group: int, idx: ^u32) -> Sql_Return {
	
	begin := idx^

	for ; idx^+1 < cast(u32)len(self.q) && 
	    !(self.q[idx^] == '*' && self.q[idx^+1] == '/'); idx^ += 1 {
		if self.q[idx^] == '\n' {
			append(&self.lf_vec, idx^)
		}
	}

	if idx^+1 >= cast(u32)len(self.q) {
		fmt.fprintf(os.stderr, "unmatched `/*'\n")
		return lex_error(self, idx^)
	}

	idx^ += 2

	append(&self.tok_vec, Token {
		    type=.Sym_Block_Comment,
		    grp=cast(u32)group,
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private)
_get_line_comment :: proc(self: ^Sql_Parser, group: int, idx: ^u32) -> Sql_Return {
	begin := idx^
	offset := strings.index_byte(self.q[idx^:], '\n')
	if offset == -1 {
		idx^ = cast(u32)len(self.q)
	} else {
		idx^ += cast(u32)offset
		append(&self.lf_vec, idx^)
		idx^ += 1
	}

	append(&self.tok_vec, Token {
		    type=.Sym_Line_Comment,
		    grp=cast(u32)group,
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private)
_get_symbol :: proc(self: ^Sql_Parser, group: int, idx: ^u32) -> Sql_Return {
	begin := idx^
	//idx^ += 1

	type : Token_Type
	ok : bool

	/* Check for 2 character symbols first */
	if begin < cast(u32)len(self.q) {
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
		return lex_error(self, idx^)
	}

	append(&self.tok_vec, Token {
		    type=type,
		    grp=cast(u32)group,
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

_is_symbol :: proc(c: u8) -> bool {
	return strings.index_byte("#()!=+-*/%~|&^.<>,;", c) >= 0
}

lex_tokenize :: proc(self: ^Sql_Parser) -> Sql_Return {
	fmt.println(self.q)
	i : u32 = 0

	append(&self.tok_vec, Token { type=.Query_Begin })

	group : int
	group_stack : [dynamic]int
	append(&group_stack, group)

	ret := 0

	for ret == 0 && i < cast(u32)len(self.q) {
		tok_len := 0
		if unicode.is_space(cast(rune)self.q[i]) {
			_skip_whitespace(self, &i)
		} else if self.q[i] == '[' {
			_get_qualified_name(self, group, &i) or_return
		} else if unicode.is_digit(cast(rune)self.q[i]) ||
		    (i+1 < cast(u32)len(self.q) && unicode.is_digit(cast(rune)self.q[i])) {
			_get_numeric(self, group, &i) or_return
		} else if self.q[i] == '@' {
			_get_variable(self, group, &i)
		} else if self.q[i] == '(' {
			i += 1
			group += 1
			append(&group_stack, group)
			append(&self.tok_vec, Token {type=.Sym_Lparen, grp=cast(u32)group, begin=i, len=1})
		} else if self.q[i] == ')' {
			if len(group_stack) == 1 {
				fmt.fprintf(os.stderr, "Unmatched ')'\n")
				return lex_error(self, i)
			}
			i += 1
			append(&self.tok_vec, Token {type=.Sym_Rparen, grp=cast(u32)group, begin=i, len=1})
			group = pop(&group_stack)
		} else if _is_symbol(self.q[i]) {
			_get_symbol(self, group, &i) or_return
		} else if self.q[i] == '_' ||
		    unicode.is_digit(cast(rune)self.q[i]) ||
		    unicode.is_alpha(cast(rune)self.q[i]) {
			_get_name(self, group, &i)
		} else {
			return lex_error(self, i)
		}
	}

	append(&self.tok_vec, Token { type = .Query_End })

	for tok in self.tok_vec {
		if enum_name, ok := fmt.enum_value_to_string(tok.type); ok {
			if tok.len > 0 {
				fmt.println(enum_name, self.q[tok.begin:tok.begin+tok.len])
			} else {
				fmt.println(enum_name)
			}
		}
	}

	return .Ok
}
