#!/bin/sh
if ! command -v grcov; then
    echo installing grcov
    cargo install grcov
fi
mkdir -p foo ./target/coverage
mkdir -p foo ./target/html-coverage
CARGO_INCREMENTAL=0 RUSTFLAGS='-Cinstrument-coverage' LLVM_PROFILE_FILE='cargo-test-%p-%m.profraw' cargo test
grcov . --binary-path ./target/debug/deps/ -s . -t lcov --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o ./target/coverage/lcov.info
grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o ./target/html-coverage/
rm *.profraw