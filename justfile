# Read dotenv-load
set dotenv-load

build:
    cargo build

test:
    cargo test

test-slowly:
    taskpolicy -c background cargo test

watch:
    fd -g '*.rs' | entr -c cargo test

run: clean
    cargo run

watch-run:
    fd -g '*.rs' | entr -c sh -c 'rm -f *.bin && cargo run'

watch-slowly:
    fd -g '*.rs' | taskpolicy -c background entr -c cargo test

watch-run-slowly:
    fd -g '*.rs' | taskpolicy -c background entr -c cargo run

clean:
    rm -f *.bin

debug: build clean
    rust-gdb target/debug/mehdb
