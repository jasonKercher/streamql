package streamql

import "core:os"
import "getargs"

main :: proc()
{
	query_str : string

	argparser := getargs.make_getargs()
	getargs.add_arg(&argparser, "h", "help", getargs.Optarg_Option.None)

	if getargs.get_flag(&argparser, "h") {
		os.write_string(os.stdout, "No help yet")
		os.exit(0)
	}

	if len(os.args) > 1 {
		if buf, ok := os.read_entire_file(os.args[1]); ok {
			query_str = string(buf)
		} else {
			os.write_string(os.stderr, "failed to open file\n")
		}
	} else {
		query_str = stdin_to_string()
	}
	
	parser : Sql_Parser

	parse_init(&parser)
	parse_parse(&parser, query_str)
}

