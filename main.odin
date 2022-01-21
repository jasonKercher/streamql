package streamql

import "core:strings"
import "core:os"
import "getargs"

main :: proc()
{
	query_str : string

	a := getargs.make_getargs()
	getargs.read_args(&a, os.args)

	cfg: bit_set[Config]

	sql: Streamql
	construct(&sql, cfg)

		query_str = "select 1"

	if exec(&sql, query_str) == .Error {
		os.exit(2)
	}

	destroy(&sql)
}

