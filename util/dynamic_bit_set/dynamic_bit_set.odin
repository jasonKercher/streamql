package dynamic_bit_set

import "core:fmt"
import "core:intrinsics"

/*
    Note that these constants are dependent on the backing being a u64.
*/
@(private="file")
INDEX_SHIFT :: 6

@(private="file")
INDEX_MASK  :: 63

Dynamic_Bit_Set :: struct {
    bits: [dynamic]u64,
    bias: int,
}

get :: proc(dbs: ^Dynamic_Bit_Set, index: $T, allocator := context.allocator) -> (res: bool, ok: bool)
    where intrinsics.type_is_integer(T) || intrinsics.type_is_enum(T) #optional_ok {

    idx := int(index) - dbs.bias

    if dbs == nil || int(index) < dbs.bias { return false, false }
    context.allocator = allocator

    leg_index := idx >> INDEX_SHIFT
    bit_index := idx &  INDEX_MASK

    resize_if_needed(dbs, leg_index) or_return

    val := u64(1 << uint(bit_index))
    res = dbs.bits[leg_index] & val == val

    return res, true
}

set :: proc(dbs: ^Dynamic_Bit_Set, index: $T, allocator := context.allocator) -> (ok: bool)
    where intrinsics.type_is_integer(T) || intrinsics.type_is_enum(T) {

    idx := int(index) - dbs.bias

    if dbs == nil || int(index) < dbs.bias { return false }
    context.allocator = allocator

    leg_index := idx >> INDEX_SHIFT
    bit_index := idx &  INDEX_MASK

    resize_if_needed(dbs, leg_index) or_return

    dbs.bits[leg_index] |= 1 << uint(bit_index)
    return true
}

create :: proc(max_index: int, min_index := 0, allocator := context.allocator) -> (res: Dynamic_Bit_Set, ok: bool) #optional_ok {
    context.allocator = allocator
    size_in_bits := max_index - min_index

    if size_in_bits < 1 { return {}, false }

    legs := size_in_bits >> INDEX_SHIFT

    res = Dynamic_Bit_Set{
        bias = min_index,
    }
    return res, resize_if_needed(&res, size_in_bits)
}

clear :: proc(dbs: ^Dynamic_Bit_Set) {
    if dbs == nil { return }
    dbs.bits = {}
}

destroy :: proc(dbs: ^Dynamic_Bit_Set) {
    if dbs == nil { return }
    delete(dbs.bits)
}

@(private="file")
resize_if_needed :: proc(dbs: ^Dynamic_Bit_Set, legs: int, allocator := context.allocator) -> (ok: bool) {
    if dbs == nil { return false }

    context.allocator = allocator

    if legs + 1 > len(dbs.bits) {
        resize(&dbs.bits, legs + 1)
    }
    return len(dbs.bits) > legs
}
