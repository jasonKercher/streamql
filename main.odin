package streamql

import "core:os"
import "getargs"
import "util"

main :: proc()
{
	query_str : string

	argparser := getargs.make_getargs()
	getargs.add_arg(&argparser, "h", "help", getargs.Optarg_Option.None)
	getargs.add_arg(&argparser, "P", "parse-only", getargs.Optarg_Option.None)
	getargs.read_args(&argparser, os.args)

	if getargs.get_flag(&argparser, "h") {
		os.write_string(os.stdout, "No help yet")
		os.exit(0)
	}

	cfg: bit_set[Config]

	if getargs.get_flag(&argparser, "P") {
		cfg += {.Parse_Only}
	}

	if len(os.args) > argparser.arg_idx {
		if buf, ok := os.read_entire_file(os.args[argparser.arg_idx]); ok {
			query_str = string(buf)
		} else {
			os.write_string(os.stderr, "failed to open file\n")
		}
		argparser.arg_idx += 1
	} else {
		query_str = util.stdin_to_string()
	}

	sql: Streamql
	construct(&sql, cfg)
	if exec(&sql, query_str) == .Error {
		os.exit(2)
	}
}

