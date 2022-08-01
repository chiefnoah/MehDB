build:
    cargo build

test:
    cargo test

test-slowly:
    taskpolicy -c background cargo test

watch:
    fd -g '*.rs'cargo test
