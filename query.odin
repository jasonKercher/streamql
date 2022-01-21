package streamql

import "core:os"
import "core:fmt"
import "core:strings"
import "core:math/bits"

Operation :: union {
	Select,
}

Query :: struct {
	plan: Plan,
}
