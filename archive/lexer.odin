package streamql

//import "bytemap"

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