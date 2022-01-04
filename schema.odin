package streamql

import "core:strings"
import "core:fmt"
import "core:os"

Schema_Props :: enum {
	Is_Var,
}

Schema :: struct {
	name: string,
	delim: string,
	rec_term: string,
	props: bit_set[Schema_Props],
}

make_schema :: proc() -> Schema {
	return Schema {}
}

destroy_schema :: proc(s: ^Schema) {
	delete(s.delim)
	delete(s.rec_term)
}

schema_resolve :: proc(sql: ^Streamql) -> Result {
	_resolve_schema_paths(sql) or_return
	for q in &sql.queries {
		if q.next_idx_ref != nil {
			q.next_idx = q.next_idx_ref^
			q.next_idx_ref = nil
		}

		if ._Delim_Set in sql.config {
			op_set_delim(&q.operation, sql.out_delim)
		}

		if ._Rec_Term_Set in sql.config {
			op_set_rec_term(&q.operation, sql.rec_term)
		}

		_resolve_query(sql, q, .Undefined)
	}

	return .Ok
}

schema_set_delim :: proc(s: ^Schema, delim: string) {
	if s == nil {
		return
	}
	delete(s.delim)
	s.delim = strings.clone(delim)
}

schema_set_rec_term :: proc(s: ^Schema, rec_term: string) {
	if s == nil {
		return
	}
	delete(s.rec_term)
	s.rec_term = strings.clone(rec_term)
}

@(private = "file")
_resolve_schema_paths :: proc(sql: ^Streamql) -> Result {
	/* Should only ever do this once */
	if ._Schema_Paths_Resolved in sql.config {
		return .Ok
	}
	sql.config += {._Schema_Paths_Resolved}

	/* SQL_SCHEMA_PATH */
	if path, ok := os.getenv("SQL_SCHEMA_PATH"); ok {
		add_schema_path(sql, path) or_return
	}

	/* $HOME/.config/streamql/schema */
	if home, ok := os.getenv("HOME"); ok {
		path := fmt.aprintf("%s/.config/streamql/schema/", home)
		add_schema_path(sql, path, false)
		delete(path)
	}

	/* TODO: figure out how to send ${datarootdir} */
	add_schema_path(sql, "/etc/streamql/schema/", false)

	return .Ok
}

@(private = "file")
_evaluate_if_const :: proc(expr: ^Expression) -> Result {
	return not_implemented()
}

_try_assign_source :: proc(col: ^Expr_Column_Name, src: ^Source) -> int {
	not_implemented()
	return 0
}

@(private = "file")
_assign_expression :: proc(expr: ^Expression, sources: []Source, strict: bool) -> Result {
	matches := 0
	sources := sources

	#partial switch v in &expr.data {
	case Expr_Case:
		return not_implemented()
	case Expr_Function:
		_assign_expressions(v.args[:], sources, strict) or_return
		function_op_resolve(&v, expr.data) or_return
		function_validate(&v, expr) or_return
		return _evaluate_if_const(expr)
	case Expr_Subquery:
		return select_resolve_type_from_subquery(expr)
	case Expr_Full_Record:
		if i32(v) != -1 {
			return .Ok
		}

		for src, i in &sources {
			if expr.table_name == "" || expr.table_name == src.alias {
				v = Expr_Full_Record(i)
				matches += 1
			}
		}
	case Expr_Column_Name:
		if v.col_idx != -1 {
			return .Ok
		}

		for src, i in &sources {
			n : int
			if expr.table_name == "" || expr.table_name == src.alias {
				n = _try_assign_source(&v, &src)
				if n > 0 {
					v.src_idx = i32(i)
				}
				if n > 1 && !strict {
					n = 1
				}
				matches += n
			}
		}

	}

	if matches > 1 {
		fmt.fprintf(os.stderr, "ambiguous expression: `%s'\n", expr.alias)
		return .Error
	}

	if matches == 0 {
		fmt.fprintf(os.stderr, "cannot find expression: `%s'\n", expr.alias)
		return .Error
	}
	return .Ok
}

@(private = "file")
_assign_expressions :: proc(exprs: []Expression, sources: []Source, strict: bool) -> Result {
	exprs := exprs
	for e in &exprs {
		_assign_expression(&e, sources, strict) or_return
	}
	return .Ok
}

@(private = "file")
_resolve_query :: proc(sql: ^Streamql, q: ^Query, union_io: Io) -> Result {
	/* First, let's resolve any subquery expressions.
	 * These should be constant values and are not
	 * tied to any parent queries
	 */
	for sub in &q.subquery_exprs {
		_resolve_query(sql, sub, union_io)
	}

	/* Top expression */
	if q.top_expr != nil {
		/* TODO: allow non-const top_expr */

	}
	return .Ok
}
