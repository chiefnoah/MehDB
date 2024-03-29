all: test build

build:
	cargo build

release:
	cargo build --release

test:
	cargo test

clean-cache:
	cargo clean

clean: clean-data
	rm -f target/release/mehdb

clean-data:
	rm -f *.bin

benchmark: clean-data release
	./target/release/mehdb
	

