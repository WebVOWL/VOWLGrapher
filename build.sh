#!/usr/bin/env bash
modes=(
    "trace"
    "dev"
    "release"
    "binary"
)
commands=(
    "MIMALLOC_VERBOSE=1 RUST_BACKTRACE=1 RUST_LOG=trace,datafusion=off,datafusion_physical_plan=off,datafusion_optimizer=off,sqlparser=off cargo leptos watch -v"
    "MIMALLOC_VERBOSE=1 RUST_BACKTRACE=1 RUST_LOG=debug,datafusion=off,datafusion_physical_plan=off,datafusion_optimizer=off,sqlparser=off cargo leptos watch -v" # --wasm-debug
    "RUST_LOG=info cargo leptos watch --release --precompress -v"
    "cargo leptos build --release --precompress -vv"
    )
help=(
    "Builds VOWL-R in trace development mode and runs it on a local server"
    "Builds VOWL-R in development mode and runs it on a local server"
    "Builds VOWL-R in production mode and runs it on a local server"
    "Builds VOWL-R in production mode, ready for deployment"
    )

valid=0
#1 Compares first input argument in argv against available modes.
for ((i=0; i < ${#modes[@]}; i++)); do
    if [[ $1 = ${modes[i]} ]]; then
        # Execute the command associated with the chosen mode
        code= eval "${commands[i]}"
        exit $code
    fi
done

#2 Checks if #1 found a valid input argument. If not, show help message.
if [[ $valid -ne 1 ]]; then
    help_string=""
    for ((i=0; i < ${#modes[@]}; i++)); do
        help_string+="    "
        help_string+=${modes[i]}
        help_string+="        "
        help_string+=${help[i]}
        help_string+=$'\n'
    done

    echo "Invalid argument '$1'. Expected one of:"
    echo "$help_string"
fi
