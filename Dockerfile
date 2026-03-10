FROM rustlang/rust:nightly-alpine AS builder

RUN apk update && apk upgrade --no-cache && apk add --no-cache \
    bash \
    curl \
    git \
    perl \
    make \
    cmake \
    ninja \
    openssl-dev \
    openssl-libs-static \
    binaryen \
    clang \
    lld \
    mold 

# Install a prebuilt binary of cargo-leptos matching version in README.md
RUN curl --proto '=https' --tlsv1.2 -LsSf https://github.com/leptos-rs/cargo-leptos/releases/download/v0.3.5/cargo-leptos-installer.sh | sh


WORKDIR /build
COPY . .

# Build mimalloc
RUN ./build_mimalloc.sh

# Override bin-target-triple defined in Cargo.toml
ENV LEPTOS_BIN_TARGET_TRIPLE="x86_64-unknown-linux-musl"

# Build VOWL-R
RUN ./build.sh binary

FROM scratch AS runner

USER 10001

# Make a directory for temporary files writable by 10001
# (Done this way as no shell command is available)
COPY --chown=10001 --from=builder --exclude=/tmp/* /tmp /tmp

WORKDIR /app

# Import VOWL-R from the build stage
COPY --chown=10001 --from=builder /build/target/x86_64-unknown-linux-musl/release/vowlr /app/
COPY --chown=10001 --from=builder /build/target/site /app/site

# Import the CAcertificates from the build stage to enable HTTPS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Set CAcertificates directory
ENV SSL_CERT_DIR=/etc/ssl/certs/

# The delay in N milli-seconds (by default 10) after which mimalloc will purge OS pages that are not in use.
# Setting N to a higher value like 100 can improve performance (sometimes by a lot) at the cost of potentially
# using more memory at times
ENV MIMALLOC_PURGE_DELAY=50

# Show statistics when the program terminates
ENV MIMALLOC_SHOW_STATS=1

# Set log level for server binary
ENV RUST_LOG="info"

# IP address the server is listening on
ENV LEPTOS_SITE_ADDR="0.0.0.0:8080"

# Set directory to serve files from by the server
ENV LEPTOS_SITE_ROOT=./site

# Depends on the port you choose
EXPOSE 8080

# Must match your final server executable name
CMD ["/app/vowlr"]
