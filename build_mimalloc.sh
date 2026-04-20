#!/bin/sh

# set -u tells the shell to treat expanding an unset parameter an error, which helps to catch e.g. typos in variable names.
# set -e tells the shell to exit if a command exits with an error (except if the exit value is tested in some other way).
#   That can be used in some cases to abort the script on error, without explicitly testing the status of each and every command.
set -eu

# Get mimalloc version from command argument.
# If none is supplied, use the default "2.2.7".
MIMALLOC_VERSION=${1:-2.2.7}

ROOT="target"
MI_PATH="$ROOT/mimalloc-$MIMALLOC_VERSION"
OUT_PATH="$MI_PATH/out"
OBJECT="mimalloc.o"
OUT_OBJECT="$OUT_PATH/$OBJECT"
LINK_LIBS="$ROOT/link_libs"


if [[ -e $OUT_OBJECT ]]; then
    echo "Mimalloc v$MIMALLOC_VERSION already installed"
    exit 0
fi

mkdir -p "$ROOT"

if [[ ! -d "$MI_PATH" ]]; then
    cd "$ROOT"

    # Fetch mimalloc source files
    echo "Downloading mimalloc v$MIMALLOC_VERSION source files"
    curl -f -L --retry 5 https://github.com/microsoft/mimalloc/archive/refs/tags/v$MIMALLOC_VERSION.tar.gz | tar xz

    cd ..
fi


mkdir -p "$OUT_PATH"

# Create mimalloc build files with the following settings
cmake -B"$OUT_PATH" -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang \
    -DMI_SECURE=OFF \
    -DMI_BUILD_OBJECT=ON \
    -DMI_BUILD_TESTS=OFF \
    -DMI_DEBUG_FULL=OFF \
    -DMI_LIBC_MUSL=ON \
    -G Ninja \
    "$MI_PATH"

# Build mimalloc
cmake --build "$OUT_PATH"

# Create directory if it doesn't already exist
mkdir -p "$LINK_LIBS"

# Create a copy of mimalloc. Used to prelink mimalloc in .cargo/config.toml
echo "Copying $OUT_OBJECT  -->  $LINK_LIBS/$OBJECT"
cp -f "$OUT_OBJECT" $LINK_LIBS/$OBJECT