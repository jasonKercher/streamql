all: streamql
SRC = $(wildcard ./*.odin)

clean:
	rm -v streamql

streamql: $(SRC)
	odin build ./ -out:streamql -debug -opt:0

release: $(SRC)
	odin build ./ -out:streamql -o:fast

