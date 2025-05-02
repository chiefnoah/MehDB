RUSTFILES=`{fd -g '*.rs'}
all:V: test build

build:V:
    cargo build

release:V:
    cargo build --release

test:V:
    cargo test

clean-cache:V:
    cargo clean

clean: clean-data
    rm -f target/release/mehdb

clean-data: *.bin
    rm -f *.bin


target/release/mehdb: $RUSTFILES
    cargo build --release

benchmark: clean-data release target/release/mehdb
    target/release/mehdb
