# Shade's binaries, its systemd user units (pointed at this package), and
# its example config and README.
{ lib, makeRustPlatform, rust-bin }:

let
  root = ../..;
  toolchain = rust-bin.fromRustupToolchainFile (root + "/rust-toolchain.toml");
  rustPlatform = makeRustPlatform {
    cargo = toolchain;
    rustc = toolchain;
  };
in
rustPlatform.buildRustPackage {
  pname = "shade";
  version = "0.1.0";

  src = lib.fileset.toSource {
    inherit root;
    fileset = lib.fileset.unions [
      (root + "/Cargo.toml")
      (root + "/Cargo.lock")
      (root + "/rust-toolchain.toml")
      (root + "/crates")
      (root + "/deploy")
      (root + "/config.example.toml")
      (root + "/README.md")
      (root + "/docs/api.md")
    ];
  };

  cargoLock.lockFile = root + "/Cargo.lock";
  cargoBuildFlags = [ "--bin" "shaded" "--bin" "shade" ];

  # The sandbox tests run real bubblewrap, which the Nix build sandbox does
  # not allow; run them with check.sh instead.
  doCheck = false;

  postInstall = ''
    install -Dm644 deploy/*.service deploy/*.timer -t $out/lib/systemd/user
    substituteInPlace $out/lib/systemd/user/*.service --replace-fail /usr/bin/ $out/bin/
    install -Dm644 README.md config.example.toml -t $out/share/doc/shade
    install -Dm644 docs/api.md $out/share/doc/shade/docs/api.md
  '';

  meta = {
    description = "Personal agent daemon (shaded) and its client (shade)";
    homepage = "https://github.com/Oery/Shade";
    mainProgram = "shade";
    platforms = lib.platforms.linux;
  };
}
