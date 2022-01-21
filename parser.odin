package streamql

//import "bytemap"

import "core:fmt"
import "core:os"
import "core:math/bits"
import "core:container/bit_array"
import "core:testing"

Parser :: struct {
	q:       string,
	lf_vec:  [dynamic]u32,
	tokens:  [dynamic]Token,
	//tok_map: bytemap.Map(Token_Type),
	consumed:bit_array.Bit_Array,
	curr:    u32,
	q_count: u32,
}

Func_Group :: enum {
	None,
	Windowed,
	Aggregate,
	Scalar,
}

make_parser :: proc() -> Parser {
	return Parser {
		lf_vec = make([dynamic]u32),
		tokens = make([dynamic]Token),
		//tok_map = bytemap.make_map(Token_Type, 256, {.No_Case}),
		consumed = bit_array.create(8),      /* why arg here? */
	}
}

destroy_parser :: proc(p: ^Parser) {
	delete(p.lf_vec)
	delete(p.tokens)
	//bytemap.destroy(&p.tok_map)
}
