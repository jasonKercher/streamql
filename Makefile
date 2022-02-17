all: debug
SRC = $(wildcard ./*.odin)

clean:
	-@rm -v sql streamql

vet: $(SRC)
	odin build ./ -out:sql -debug -opt:0 -vet

debug: $(SRC)
	odin build ./ -out:sql -opt:0 -debug

release: $(SRC)
	odin build ./ -out:sql -o:speed


check: $(SRC)
	odin test ./ -opt:0 -debug

