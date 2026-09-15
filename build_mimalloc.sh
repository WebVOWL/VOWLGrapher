#!/bin/sh

# set -u tells the shell to treat expanding an unset parameter an error, which helps to catch e.g. typos in variable names.
# set -e tells the shell to exit if a command exits with an error (except if the exit value is tested in some other way).
#   That can be used in some cases to abort the script on error, without explicitly testing the status of each and every command.
set -eu

# Get mimalloc version from command argument.
# If none is supplied, use the default "3.5.2".
MIMALLOC_VERSION=${1:-3.5.2}

# Fetch mimalloc source files
curl -f -L --retry 5 https://github.com/microsoft/mimalloc/archive/refs/tags/v$MIMALLOC_VERSION.tar.gz | tar xz

cd mimalloc-$MIMALLOC_VERSION

mkdir out

# Create mimalloc build files with the following settings
cmake -Bout -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang \
    -DMI_SECURE=OFF \
    -DMI_OPT_ARCH=ON \
    -DMI_ALLOW_THP=OFF \
    -DMI_BUILD_OBJECT=ON \
    -DMI_BUILD_TESTS=OFF \
    -DMI_DEBUG_FULL=OFF \
    -DMI_LIBC_MUSL=ON \
    -G Ninja \
    .

# Build mimalloc
cmake --build out

cd ..

# Create directory if it doesn't already exist
mkdir -p link_libs

# Create a copy of mimalloc. Used to prelink mimalloc in .cargo/config.toml
cp -f mimalloc-$MIMALLOC_VERSION/out/mimalloc.o link_libs/mimalloc.o