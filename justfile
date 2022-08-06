build:
    cargo build

test:
    cargo test

test-slowly:
    taskpolicy -c background cargo test

watch:
    fd -g '*.rs' | entr -c cargo test

watch-run:
    fd -g '*.rs' | entr -c cargo run

watch-slowly:
    fd -g '*.rs' | taskpolicy -c background entr -c cargo test

watch-run-slowly:
    fd -g '*.rs' | taskpolicy -c background entr -c cargo run
