{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };
  outputs = {
    self,
    nixpkgs,
    utils,
    rust-overlay,
  }:
    utils.lib.eachDefaultSystem (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
      in {
        devShell = pkgs.mkShell {
          shellHook = ''
            export PKG_CONFIG_PATH="${pkgs.openssl.dev}/lib/pkgconfig"
          '';
          buildInputs = with pkgs; [
            (rust-bin.fromRustupToolchainFile
              ./rust-toolchain.toml)
            leptosfmt
            cargo-leptos
            wasm-bindgen-cli_0_2_118
            clang
            mold
            sass
            tailwindcss_4
            openssl
            pkgconf
          ];
        };
      }
    );
}
