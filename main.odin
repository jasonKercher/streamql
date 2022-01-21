package streamql

import "core:strings"
import "core:os"
import "getargs"

main :: proc()
{
	query_str : string

	a := getargs.make_getargs()
	getargs.add_arg(&a, "h", "help", getargs.Optarg_Option.None)
	getargs.add_arg(&a, "P", "parse-only", getargs.Optarg_Option.None)
	getargs.add_arg(&a, "", "schema-path", getargs.Optarg_Option.Required)
	getargs.read_args(&a, os.args)

	if getargs.get_flag(&a, "h") {
		os.write_string(os.stdout, "No help yet")
		os.exit(0)
	}

	cfg: bit_set[Config]
	if getargs.get_flag(&a, "P") {
		cfg += {.Parse_Only}
	}

	sql: Streamql
	construct(&sql, cfg)
	if path, ok := getargs.get_payload(&a, "schema-path"); ok {
		if add_schema_path(&sql, path) == .Error {
			os.exit(2)
		}
	}

	if len(os.args) > a.arg_idx {
		if buf, ok := os.read_entire_file(os.args[a.arg_idx]); ok {
			query_str = string(buf)
		} else {
			os.write_string(os.stderr, "failed to open file\n")
		}
		a.arg_idx += 1
	} else {
		query_str = "select 1"
	}

	if exec(&sql, query_str) == .Error {
		os.exit(2)
	}

	getargs.destroy(&a)
	destroy(&sql)
}

