FROM alpine:edge AS builder

#### Rust Installation ####
# The official `rustlang/rust:nightly-alpine` image does not support 
# the LLVM version bundled with the latest nightly Rust.
# This is due to Alpine:v3.23 only shipping up to Clang21, but Clang22 is required.
# Rust install taken from: https://github.com/rust-lang/docker-rust/blob/78eddf7aaa7cb9dd12d6e95605301d8a9f74290e/nightly/alpine3.23/Dockerfile

RUN apk add --no-cache \
    ca-certificates \
    musl-dev \
    gcc

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=nightly

RUN set -eux; \
    \
    arch="$(apk --print-arch)"; \
    case "$arch" in \
    'x86_64') \
    rustArch='x86_64-unknown-linux-musl'; \
    rustupSha256='9cd3fda5fd293890e36ab271af6a786ee22084b5f6c2b83fd8323cec6f0992c1'; \
    ;; \
    'aarch64') \
    rustArch='aarch64-unknown-linux-musl'; \
    rustupSha256='88761caacddb92cd79b0b1f939f3990ba1997d701a38b3e8dd6746a562f2a759'; \
    ;; \
    'ppc64le') \
    rustArch='powerpc64le-unknown-linux-musl'; \
    rustupSha256='e15d033af90b7a55d170aac2d82cc28ddd96dbfcdda7c6d4eb8cb064a99c4646'; \
    ;; \
    *) \
    echo >&2 "unsupported architecture: $arch"; \
    exit 1; \
    ;; \
    esac; \
    \
    url="https://static.rust-lang.org/rustup/archive/1.29.0/${rustArch}/rustup-init"; \
    wget "$url"; \
    echo "${rustupSha256} *rustup-init" | sha256sum -c -; \
    \
    chmod +x rustup-init; \
    ./rustup-init -y --no-modify-path --profile minimal --default-toolchain $RUST_VERSION --default-host ${rustArch}; \
    rm rustup-init; \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME; \
    \
    rustup --version; \
    cargo --version; \
    rustc --version;


#### VOWLGrapher Installation ####
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
RUN curl --proto '=https' --tlsv1.2 -LsSf https://github.com/leptos-rs/cargo-leptos/releases/download/v0.3.6/cargo-leptos-installer.sh | sh

WORKDIR /build
COPY . .

# Build mimalloc
RUN ./build_mimalloc.sh

# Override bin-target-triple defined in Cargo.toml
ENV LEPTOS_BIN_TARGET_TRIPLE="x86_64-unknown-linux-musl"

# Build VOWLGrapher
RUN ./build.sh binary



FROM scratch AS runner

USER 10001

# Make a directory for temporary files writable by 10001
# (Done this way as no shell command is available)
COPY --chown=10001 --from=builder --exclude=/tmp/* /tmp /tmp

WORKDIR /app

# Import VOWLGrapher from the build stage
COPY --chown=10001 --from=builder /build/target/x86_64-unknown-linux-musl/release/vowlgrapher /app/
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
CMD ["/app/vowlgrapher"]
