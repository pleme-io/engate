{
  description = "engate — typed producer↔consumer attach primitive (eliminates the attach-race bug class by construction)";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-25.11";
    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, crate2nix, fenix, substrate, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      eachSystem = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = eachSystem (system:
        let
          pkgs = import nixpkgs { inherit system; };
          rustChannel = fenix.packages.${system}.stable;
          rustToolchain = rustChannel.withComponents [
            "cargo"
            "rustc"
            "rust-src"
            "rust-analyzer"
            "clippy"
            "rustfmt"
          ];
        in
        {
          default = pkgs.mkShell {
            buildInputs = [
              rustToolchain
              pkgs.crate2nix
              pkgs.cargo-edit
              pkgs.cargo-watch
            ];
            shellHook = ''
              echo "engate workspace — typed attach primitive"
              echo "  cargo test                       # all crates"
              echo "  cargo test --features loom       # exhaustive interleavings (M2)"
            '';
          };
        });

      formatter = eachSystem (system:
        (import nixpkgs { inherit system; }).nixfmt-rfc-style);
    };
}
