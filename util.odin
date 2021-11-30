package streamql

import "core:os"
import "core:io"
import "core:fmt"
import "core:bufio"
import "core:strings"

stdin_to_string :: proc() -> string
{
	b : strings.Builder
	strings.init_builder(&b)
	stdin_reader, ok := io.to_reader(os.stream_from_handle(os.stdin))
	if (!ok) {
		os.write_string(os.stderr, "Failed to build reader from stdin");
		os.exit(1)
	}

	buffered_input : bufio.Reader
	bufio.reader_init(&buffered_input, stdin_reader)

	c,err := bufio.reader_read_byte(&buffered_input)
	for ; err == .None ; c,err = bufio.reader_read_byte(&buffered_input) {
		/* Hmmm... Not breaking on c-d */
		//if c == 0x04 {
		//	break
		//}
		append(&b.buf, c)
	}

	return strings.to_string(b)
}

