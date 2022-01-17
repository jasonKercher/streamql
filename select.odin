package streamql

import "core:strings"

Select :: struct {
	schema: Schema,
	writer: Writer,
	expressions: [dynamic]Expression,
	top_count: i64,
}

make_select :: proc() -> Select {
	return Select {
		expressions = make([dynamic]Expression),
	}
}

select_add_expression :: proc(s: ^Select, expr: ^Expression) -> ^Expression {
	append(&s.expressions, expr^)
	return &s.expressions[len(s.expressions) - 1]
}

select_apply_alias :: proc(s: ^Select, alias: string) {
	expr := &s.expressions[len(s.expressions) - 1]
	expr.alias = strings.clone(alias)
}

select_resolve_type_from_subquery :: proc(expr: ^Expression) -> Result {
	return not_implemented()
}

select_expand_asterisks :: proc(q: ^Query, force: bool) {
	sel := &q.operation.(Select)
	for expr, i in &sel.expressions {
		aster, is_aster := expr.data.(Expr_Asterisk)
		if !is_aster {
			continue
		}

		/* Ideally, we do not expand the asterisk.  No need to
		 * parse anything, if we are allowed to just take the
		 * whole line.
		 */
		src_idx := int(aster)
		_, is_subq := q.sources[src_idx].data.(^Query)
		if !is_subq && !force && q.sub_id == 0 && schema_eq(&q.sources[src_idx].schema, &sel.schema) {
			continue
		}

		_expand_asterisk(sel, &q.sources[src_idx], i)
	}

	/*** stopped here... ***/
}

select_verify_must_run :: proc(sel: ^Select) {
	not_implemented()
}

select_apply_process :: proc(q: ^Query, is_subquery: bool) -> Result {
	return not_implemented()
}

_expand_asterisk :: proc(sel: ^Select, src: ^Source, idx: int) {
	not_implemented()
}
