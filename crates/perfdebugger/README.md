# Performance Debugging

This crate runs all code of VOWLGrapher natively to take advantage
of the suite of debugging and performance optimization tools available in Rust.

## Building and Running

To build this crate with profiling, open a terminal and run: `cargo build -p perfdebugger --profile profiling --target "x86_64-unknown-linux-gnu" --all-features --workspace`

To build this crate in standard release mode, open a terminal and run: `cargo build -p perfdebugger --release --target "x86_64-unknown-linux-gnu" --all-features --workspace`

Note that it does take a while to compile it.

To start the binary with profiling enabled, run: `VOWLGRAPHER_MAX_INPUT_SIZE_BYTES=5000000000 VOWLGRAPHER_RESOLVE_IMPORTS=false RUST_BACKTRACE=1 RUST_LOG=info ./target/x86_64-unknown-linux-gnu/profiling/perfdebugger <path/to/ontology>`

To start the binary in release mode, run: `VOWLGRAPHER_MAX_INPUT_SIZE_BYTES=5000000000 VOWLGRAPHER_RESOLVE_IMPORTS=false RUST_BACKTRACE=1 RUST_LOG=info ./target/x86_64-unknown-linux-gnu/release/perfdebugger <path/to/ontology>`
