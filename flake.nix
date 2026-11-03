{
  description = "Shade: a personal agent daemon (shaded) and its client (shade)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # Builds with the toolchain pinned in rust-toolchain.toml, whatever nixpkgs ships.
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    { self, nixpkgs, rust-overlay }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forEachSystem =
        build:
        nixpkgs.lib.genAttrs systems (
          system:
          build (import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          })
        );
    in
    {
      packages = forEachSystem (pkgs: {
        default = pkgs.callPackage ./packaging/nix/package.nix { };
      });

      overlays.default = final: prev: {
        shade = self.packages.${prev.stdenv.hostPlatform.system}.default;
      };

      nixosModules.default = import ./packaging/nix/module.nix self;
    };
}
