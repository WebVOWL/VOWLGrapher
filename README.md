# VOWL-R: WebVOWL Reimagined

This branch tracks development of VOWL-R, which is a total rewrite of WebVOWL in Rust.

## Run using Docker

Pull image: `docker pull ghcr.io/webvowl/vowl-r:latest`

Or use the [docker compose file](/docker-compose.yml) with command `docker-compose up -d`

### Building the docker image

0. Make sure Docker is installed
1. Clone the project locally, e.g. `git clone https://github.com/WebVOWL/VOWL-R.git`
2. Make sure you're in the VOWL-R folder, e.g. `cd VOWL-R`
3. To build the docker image run `docker build . -t vowlr-dev`
4. To start the docker image run `docker run -p 8080:8080 vowlr-dev`
5. Visit [http://localhost:8080](http://localhost:8080) to use VOWL-R

## Development setup

> [!NOTE]
> Using Linux is recommended

0. Clone the project locally, e.g. `git clone https://github.com/WebVOWL/VOWL-R.git`
1. Install Rust from [https://www.rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)
2. Install the following:
    ```bash
     apt install clang mold make cmake libssl-dev pkg-config
    ```
3. Run `cargo install leptosfmt`
4. Run `cargo install --locked cargo-leptos --version 0.3.2`
    > If you get a compile error `Can't locate FindBin.pm in @INC` you can either install Perl (e.g. `dnf install perl`) or [download a prebuilt binary](https://github.com/leptos-rs/cargo-leptos/releases/latest)
5. Use the convenience shell file `build.sh` to build the project with different profiles based on the supplied argument. E.g. to build and run a development server, run `./build.sh dev`

## Environment variables

<details>
<summary>Help defining environment variables</summary>
Environment variables are defined like this:

```
<key=value> <key=value> ... <path/to/server/binary>
```

For instance:

```bash
VOWLGRAPHER_MAX_INPUT_SIZE_BYTES=50000000 RUST_BACKTRACE=1 RUST_LOG=info ./target/x86_64-unknown-linux-gnu/debug/vowlgrapher
```

</details>

The following environment variables are available:

|              Variable              | Type  |    Default value    | Description                                                        |
| :--------------------------------: | :---: | :-----------------: | :----------------------------------------------------------------- |
| `VOWLGRAPHER_MAX_INPUT_SIZE_BYTES` | Bytes | `52,428,800` (50MB) | The maximum allowed size, in bytes, of any input into VOWLGrapher. |
