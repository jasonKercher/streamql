package streamql

import "core:math/bits"
import "core:strings"
import "core:fmt"
import "core:os"

//import "bytemap"

Schema_Props :: enum {
Is_Var,
Is_Default,
Is_Preresolved,
Delim_Set,
}

Schema_Item :: struct {
name: string,
loc: i32,
width: i32,
}

Schema :: struct {
reader: Reader,
layout: [dynamic]Schema_Item,
//item_map: bytemap.Multi(i32),
name: string,
schema_path: string,
delim: string,
rec_term: string,
props: bit_set[Schema_Props],
}

make_schema :: proc() -> Schema {
return Schema {
	props = {.Is_Default},
}
}

destroy_schema :: proc(s: ^Schema) {
delete(s.delim)
delete(s.rec_term)
}

schema_eq :: proc(s1: ^Schema, s2: ^Schema) -> bool {
not_implemented()
return true
}

schema_copy :: proc(dest: ^Schema, src: ^Schema) {
if src == nil {
	if .Delim_Set not_in dest.props {
		schema_set_delim(dest, ",")
	}
	dest.props += {.Is_Default}
	return
}

if .Delim_Set not_in src.props {
	schema_set_delim(dest, src.delim)
}

if .Is_Default in src.props {
	dest.props += {.Is_Default}
} else {
	dest.props -= {.Is_Default}
}
}

schema_get_item :: proc(s: ^Schema, key: string) -> (Schema_Item, Result) {
//indices, found := bytemap.get(&s.item_map, key)
indices: []i32 = {1}
found := false
if !found {
	return Schema_Item { loc = -1 }, .Ok
}
if len(indices) > 1 {
	fmt.fprintf(os.stderr, "expression `%s' ambiguous\n", key)
	return Schema_Item { loc = -1 }, .Error
}
return s.layout[indices[0]], .Ok
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

	_resolve_query(sql, q)
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

schema_assign_header :: proc(src: ^Source, src_idx: int) -> Result {
	return not_implemented()
}

schema_preflight :: proc(s: ^Schema) {
	//if s == nil {
	//	return
	//}

	/* May be called already from order.odin */
	//if len(s.item_map.values) > 0 {
	//	return
	//}

	//s.item_map = bytemap.make_multi(i32, u64(len(s.layout) * 2), {.No_Case})

	for it, i in &s.layout {
		it.loc = i32(i)
		//bytemap.set(&s.item_map, it.name, it.loc)
	}

	if .Delim_Set not_in s.props {
		schema_set_delim(s, ",");
	}

	if len(s.rec_term) == 0 {
		s.rec_term = "\n"
	}
}

@private
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

@private
_evaluate_if_const :: proc(expr: ^Expression) -> Result {
	fn := &expr.data.(Expr_Function)
	for expr in fn.args {
	}

	expr.fn_bak = new(Expr_Function)
	expr.fn_bak^ = fn^

	return .Ok
}

@private
_try_assign_source :: proc(col: ^Expr_Column_Name, src: ^Source) -> int {
	not_implemented()
	return 0
}

@private
_assign_expression :: proc(expr: ^Expression, sources: []Source, strict: bool = true) -> Result {
	matches := 0
	sources := sources

	#partial switch v in &expr.data {
	case Expr_Function:
		_assign_expressions(&v.args, sources, strict) or_return
		//function_op_resolve(&v, expr.data) or_return
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
	case:
		return .Ok
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

@private
_assign_expressions :: proc(exprs: ^[dynamic]Expression, sources: []Source, strict: bool = true) -> Result {
	if exprs == nil {
		return .Ok
	}
	exprs := exprs
	for e in exprs {
		_assign_expression(&e, sources, strict) or_return
	}
	return .Ok
}

@private
_assign_logic_group_expressions :: proc(lg: ^Logic_Group, sources: []Source, strict: bool = true) -> Result {
	if lg == nil {
		return .Ok
	}

	switch lg.type {
	case .And:
		fallthrough
	case .Or:
		_assign_logic_group_expressions(lg.items[0], sources, strict) or_return
		return _assign_logic_group_expressions(lg.items[1], sources, strict)
	case .Not:
		return _assign_logic_group_expressions(lg.items[0], sources, strict)
	case .Predicate:
		fallthrough
	case .Predicate_Negated:
		_assign_expression(&lg.condition.exprs[0], sources, strict) or_return
		return _assign_expression(&lg.condition.exprs[1], sources, strict)
	}

	return .Ok
}

@private
_load_schema_by_name :: proc(sql: ^Streamql, src: ^Source, src_idx: int) -> Result {
	return not_implemented()
}

@private
_resolve_file :: proc(sql: ^Streamql, q: ^Query, src: ^Source) -> Result {
	return not_implemented()
}


@private
_resolve_source :: proc(sql: ^Streamql, q: ^Query, src: ^Source, src_idx: int) -> Result {
	//if len(src.schema.item_map.values) != 0 {
	//	return .Ok
	//}

	if src.schema.name == "" && sql.default_schema != "" {
		src.schema.name = strings.clone(sql.default_schema)
	}
	if src.schema.name != "" {
		/* TODO: case_insensitive */
		if src.schema.name != "default" {
			src.schema.props -= {.Is_Default}
			src.schema.reader.skip_rows = 0
			_load_schema_by_name(sql, src, src_idx) or_return
		}
	}

	switch v in src.data {
	case ^Query:
		//if src.alias == "" { throw for missing alias?? }
		_resolve_query(sql, v)
		select := v.operation.(Select)
		src.schema = select.schema
		src.schema.props += {.Is_Preresolved}
	case string:
		_resolve_file(sql, q, src) or_return
		if .Is_Default in src.schema.props {
		}
	}

	reader_assign(sql, src) or_return

	//rec: Record
	src.schema.reader.max_idx = bits.I32_MAX
	//src.schema.reader.get_record__(&src.schema.reader, &rec)
	src.schema.reader.max_idx = 0

	if .Is_Stdin not_in src.props {
		src.schema.reader.reset__(&src.schema.reader)
	}

	/* if we've made it this far, we want to try
	 * and determine schema by reading the top
	 * row of the file and assume a delimited
	 * list of field names.
	 */
	if .Is_Default in src.schema.props {
		if .Is_Preresolved not_in src.schema.props {
			//schema_assign_header(src, &rec, src_idx)
		}
	} else {
		//new_size := 1 if len(rec.fields) == 0 else len(rec.fields)
	}
	
	schema_preflight(&src.schema)

	if .Is_Default in src.schema.props || .Is_Stdin in src.props {
		//destroy_record(&rec)
	} else {
		//src.schema.reader.first_rec = rec
	}

	return .Ok
}

@private
_resolve_unions :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if len(q.unions) == 0 {
		return .Ok
	}
	return not_implemented()
}

@private
_resolve_asterisk :: proc(exprs: ^[dynamic]Expression, sources: []Source) -> Result {
	sources := sources
	for i := 0; i < len(exprs); i += 1 {
		idx := i
		if _, is_aster := exprs[i].data.(Expr_Asterisk); !is_aster {
			continue
		}

		matches: int
		for src, j in &sources {
			if exprs[i].table_name == "" || exprs[i].table_name == src.alias {
				if matches > 0 {
					new_expr := make_expression(Expr_Asterisk(j))
					i += 1
					insert_at(exprs, i, new_expr)
				} else {
					aster := exprs[i].data.(Expr_Asterisk)
					aster = Expr_Asterisk(j)
				}
				matches += 1
			}
		}
		if matches == 0 {
			fmt.fprintf(os.stderr, "failed to locate table `%s'\n", exprs[i].table_name)
			return .Error
		}
	}
	return .Ok
}

@private
_map_groups :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return not_implemented()
}

@private
_group_validate_having :: proc(q: ^Query, is_summarize: bool) -> Result {
	return not_implemented()
}

@private
_group_validation :: proc(q: ^Query, exprs, op_exprs: ^[dynamic]Expression, is_summarize: bool) -> Result {
	return not_implemented()
}

@private
_resolve_query :: proc(sql: ^Streamql, q: ^Query) -> Result {
	/* First, let's resolve any subquery expressions.
	 * These should be constant values and are not
	 * tied to any parent queries
	 */

	is_strict := .Strict in sql.config

	/* Top expression */
	if q.top_expr != nil {
		_assign_expression(q.top_expr, nil, is_strict) or_return
	}

	/* If there is an order by, make sure NOT to send top_count
	 * to the operation. However, if there is a union, this does
	 * not apply.  If there is a union, the top count belongs to
	 * the operation (select can be assumed). The only goal is to
	 * make sure ALL the selected records are ordered
	 */
		op_set_top_count(&q.operation, q.top_count)

	/* Now, we should verify that all sources
	 * exist and populate schemas.  As we loop, we
	 * resolve the expressions that are listed in join
	 * clauses because of the following caveat:
	 *
	 * SELECT *
	 * FROM T1
	 * JOIN T2 ON T1.FOO = T3.FOO -- Cannot read T3 yet!
	 * JOIN T3 ON T2.FOO = T3.FOO
	 */
	for src, i in &q.sources {
		_resolve_source(sql, q, &src, i) or_return
		source_resolve_schema(sql, &src) or_return


		if src.join_logic != nil {
			_assign_logic_group_expressions(src.join_logic, q.sources[:i+1], is_strict) or_return
		}

		if i > 0 && .Force_Cartesian not_in sql.config {
		}
	}

	op_schema := op_get_schema(&q.operation)

	/* Where clause */
	_assign_logic_group_expressions(q.where_, q.sources[:], is_strict) or_return

	/* Validate operation expressions */
	op_exprs := op_get_expressions(&q.operation)
	if _, is_select := q.operation.(Select); is_select {
		_resolve_asterisk(op_exprs, q.sources[:]) or_return
	}
	_assign_expressions(op_exprs, q.sources[:], is_strict) or_return

	op_add_exprs := op_get_additional_expressions(&q.operation)
	_assign_expressions(op_add_exprs, q.sources[:], is_strict) or_return

	/* Validate HAVING expressions */
	_assign_logic_group_expressions(q.having, q.sources[:], is_strict) or_return

	/* Do GROUP BY last. There are less caveats having
	 * waited until everything else is resolved
	 */
	_resolve_unions(sql, q) or_return
	op_writer_init(sql, q) or_return

	schema_preflight(op_schema)

	if q.into_table_name == "" {
		return .Ok
	}

	/* If this query will be writing changes to the file system,
	 * we need to be aware of this when parsing future queries.
	 * These are mapped as absolute paths. First check that the
	 * file exists. If it doesn't, create it now so that realpath
	 * works. Creating the file also has the (undesireable?) affect
	 * of making fuzzy file discovery possible on a file that did
	 * not previously exist. TODO
	 */
	if !os.is_file(q.into_table_name) {
		/* NOTE: We may enter this block for a number of reasons.
		 *       We will rely on open for catching errors.
		 */
		fd, err := os.open(q.into_table_name, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0o664)
		if err != os.ERROR_NONE {
			fmt.fprintf(os.stderr, "failed to create file `%s'", q.into_table_name)
			return .Error
		}
		err = os.close(fd)
		if err != os.ERROR_NONE {
			fmt.fprintf(os.stderr, "failed to close file `%s'", q.into_table_name)
			return .Error
		}
	} else if _, is_sel := q.operation.(Select); is_sel && .Overwrite in sql.config {
		fmt.fprintf(os.stderr, "cannot SELECT INTO: `%s' already exists\n", q.into_table_name)
		return .Error
	}
	path, err := os.absolute_path_from_relative(q.into_table_name)

	sql.schema_map[path] = op_schema

	return .Ok
}
