{
  description = "engate — typed producer↔consumer attach primitive (eliminates the attach-race bug class by construction)";

  inputs = {
    substrate.url = "github:pleme-io/substrate";
    nixpkgs.follows = "substrate/nixpkgs";
  };

  outputs =
    { substrate, nixpkgs, ... }:
    let
      workspace = substrate.rust.workspace {
        src = ./.;
        member = "engate-attach";
        devShellPackages = [
          "crate2nix"
          "cargo-edit"
          "cargo-watch"
        ];
        devShellHook = ''
          echo "engate workspace — typed attach primitive"
          echo "  cargo test                       # all crates"
          echo "  cargo test --features loom       # exhaustive interleavings (M2)"
        '';
      };
    in
    workspace
    // {
      formatter = nixpkgs.lib.genAttrs (builtins.attrNames workspace.packages) (
        system: nixpkgs.legacyPackages.${system}.nixfmt
      );
    };
}
