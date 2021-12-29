package streamql

Expression_Type :: enum {
	Undefined,
	Column_Name,
	Switch_Case,
	Row_Number,
	Full_Record,
	Reference,
	Variable,
	Grouping,
	Const,
	Null,
	Asterisk,
	Function,
	Aggregate,
	Subquery,
}

Expression :: struct {
	type: Expression_Type,
}
