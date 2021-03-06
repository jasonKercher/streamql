package streamql

import "core:fmt"
import "core:os"
import "core:os/os2"
import "getargs"
import "util"

main :: proc()
{
	context.allocator = os2.heap_allocator()
	query_str : string

	a := getargs.make_getargs()
	defer getargs.destroy(&a)

	getargs.add_arg(&a, "", "help", .None)
	getargs.add_arg(&a, "", "api", .None)
	getargs.add_arg(&a, "b", "char-as-byte", .None)
	getargs.add_arg(&a, "c", "check", .None)
	getargs.add_arg(&a, "h", "no-header", .None)
	getargs.add_arg(&a, "H", "add-header", .None)
	getargs.add_arg(&a, "v", "verbose", .None)
	getargs.add_arg(&a, "P", "parse-only", .None)
	getargs.add_arg(&a, "p", "print-plan", .None)
	getargs.add_arg(&a, "", "schema-path", .Required)
	getargs.read_args(&a, os.args)

	if getargs.get_flag(&a, "help") {
		os.write_string(os.stdout, "No help yet")
		os.exit(0)
	}

	cfg: bit_set[Config] = {}
	if getargs.get_flag(&a, "char-as-byte") {
		cfg += {.Char_As_Byte}
	}
	if getargs.get_flag(&a, "check") {
		cfg += {.Check}
	}
	if getargs.get_flag(&a, "no-header") {
		cfg += {.No_Header}
	}
	if getargs.get_flag(&a, "add-header") {
		cfg += {.Add_Header}
	}
	if getargs.get_flag(&a, "parse-only") {
		cfg += {.Parse_Only}
	}
	if getargs.get_flag(&a, "print-plan") {
		cfg += {.Print_Plan}
	}

	sql: Streamql
	construct(&sql, cfg)
	defer destroy(&sql)

	if getargs.get_flag(&a, "verbose") {
		sql.verbosity = .Noisy
	}

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
		query_str = util.stdin_to_string()
	}

	if !getargs.get_flag(&a, "api") {
		if exec(&sql, query_str) == .Error {
			os.exit(2)
		}
		os.exit(0)
	}

	/* Using API from here below */
	if generate_plans(&sql, query_str) == .Error {
		os.exit(2)
	}

	fields, res := step(&sql)
	for ; res == .Running; fields, res = step(&sql) {
		first := true
		for f in fields {
			if !first {
				fmt.print(',')
			}
			first = false
			switch v in f.data {
			case i64:
				fmt.printf("%d", v)
			case f64:
				fmt.printf("%f", v)
			case string:
				fmt.print(v)
			}
		}
		fmt.println()
	}
}

