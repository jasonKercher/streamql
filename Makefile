all: streamql
SRC = $(wildcard ./*.odin)

clean:
	-@rm -v streamql

vet: $(SRC)
	odin build ./ -out:streamql -debug -opt:0 -vet

streamql: $(SRC)
	odin build ./ -out:streamql -debug -opt:0

release: $(SRC)
	odin build ./ -out:streamql -o:fast

