all: streamql

clean:
	rm -v streamql

streamql:
	odin build ./ -out:streamql -debug -opt:0

release:
	odin build ./ -out:streamql -o:fast

