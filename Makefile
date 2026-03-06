.PHONY: build test shell clean

build:
	nix develop --no-pure-eval -c dune build

test:
	nix develop --no-pure-eval -c dune runtest

shell:
	nix develop --no-pure-eval

clean:
	dune clean
