package getargs

import "core:fmt"
import "core:os"

@(private="file")
Optarg :: union {
	string,
	bool,
}

Optarg_Option :: enum { None, Required, Optional }

@(private="file")
Argument :: struct {
	option:  Optarg_Option,
	payload: Optarg,
}

Getargs_Option :: enum { No_Dash, Short_As_Long }

Getargs :: struct {
	arg_map:  map[string]int,
	arg_vec:  [dynamic]Argument,
	arg_opts: bit_set[Getargs_Option],
	arg_idx:  int,
}

make_getargs :: proc (getargs_opts: bit_set[Getargs_Option] = {}) -> Getargs {
	return Getargs { 
		arg_map=make(map[string]int),
		arg_vec=make([dynamic]Argument),
		arg_idx=1,
		arg_opts=getargs_opts,
	}
}

construct :: proc (self: ^Getargs, getargs_opts: bit_set[Getargs_Option] = {}) {
	self^ = {
		arg_map=make(map[string]int),
		arg_vec=make([dynamic]Argument),
		arg_idx=1,
		arg_opts=getargs_opts,
	}
}

destroy :: proc(self: ^Getargs) {
	delete(self.arg_vec)
	delete(self.arg_map)
}

@(private="file")
_parse_short_args :: proc(self: ^Getargs, args: []string, dash_offset: int) {
	i := dash_offset
	for ; i < len(args[self.arg_idx]); i += 1 {
		idx, ok := self.arg_map[args[self.arg_idx][i:i+1]]
		if !ok {
			fmt.fprintf(os.stderr, "unable to find arg `%s'\n", args[self.arg_idx][i:i+1])
			os.exit(1)
		}
		
		arg := &self.arg_vec[idx]
		
		if (arg.option == .None) {
			arg.payload = true
			continue
		}

		if i+1 < len(args[self.arg_idx]) {
			arg.payload = args[self.arg_idx][i+1:]
			if arg.option == .Optional || len(arg.payload.(string)) > 0 {
				return
			}
			fmt.fprintf(os.stderr, "`%c' expects an argument\n", args[self.arg_idx][i])
			os.exit(1)
		}

		if self.arg_idx + 1 >= len(args) || args[self.arg_idx+1][0] == '-' {
			if arg.option == .Optional {
				arg.payload = true
				return
			}

			fmt.fprintf(os.stderr, "`%c' expects an argument\n", args[self.arg_idx][i])
			os.exit(1)
		}

		self.arg_idx += 1
		arg.payload = args[self.arg_idx]

		break;
	}
}

@(private="file")
_parse_long_arg :: proc(self: ^Getargs, args: []string, dash_offset: int) {
	arg_name : string
	has_optarg : bool

	i := dash_offset
	for ; i < len(args[self.arg_idx]); i += 1 {
		if args[self.arg_idx][i] == '=' {
			arg_name = args[self.arg_idx][dash_offset:i]
			has_optarg = true
			break;
		}
	}

	if !has_optarg {
		arg_name = args[self.arg_idx][dash_offset:]
	}

	idx, ok := self.arg_map[arg_name]
	if !ok {
		fmt.fprintf(os.stderr, "unable to find arg `%s'\n", arg_name)
		os.exit(1)
	}

	arg := &self.arg_vec[idx]
	if has_optarg && arg.option == .None {
		fmt.fprintf(os.stderr, "`%s' does not expect an argument\n", arg_name)
		os.exit(1)
	}

	if arg.option == .None {
		arg.payload = true
		return
	}

	if has_optarg {
		arg.payload = args[self.arg_idx][i+1:]
		return
	}

	if self.arg_idx + 1 >= len(args) || args[self.arg_idx+1][0] == '-' {
		if arg.option == .Optional {
			arg.payload = true
			return
		}

		fmt.fprintf(os.stderr, "`%s' expects an argument\n", arg_name)
		os.exit(1)
	}

	self.arg_idx += 1
	arg.payload = args[self.arg_idx]
}

read_args :: proc (self: ^Getargs, args : []string) {

	dash_offset: int = 1
	
	if .No_Dash in self.arg_opts {
		dash_offset = 0
	}

	for ; self.arg_idx < len(args); self.arg_idx += 1 {
		/* Check if arg at all */
		if args[self.arg_idx][0] != '-' && .No_Dash not_in self.arg_opts {
			return
		}

		/* Check if long arg */
		if len(args[self.arg_idx]) > dash_offset+1 && args[self.arg_idx][dash_offset] == '-' {
			_parse_long_arg(self, args, dash_offset+1)
			continue
		}

		if .Short_As_Long in self.arg_opts {
			_parse_long_arg(self, args, dash_offset)
		} else {
			_parse_short_args(self, args, dash_offset)
		}
	}
}
