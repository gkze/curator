{
  description = "Curator";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-compat.url = "https://flakehub.com/f/edolstra/flake-compat/1.tar.gz";
    flakelight = {
      url = "github:nix-community/flakelight";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    devshell = {
      url = "github:numtide/devshell";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    git-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      devshell,
      flakelight,
      git-hooks,
      treefmt-nix,
      crane,
      rust-overlay,
      ...
    }@inputs:
    flakelight ./. (
      { lib, ... }:
      let
        # Single source of truth: reads from rust-toolchain.toml
        mkRustToolchain = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

        # Shared build dependencies
        mkBuildInputs =
          pkgs:
          with pkgs;
          [
            sqlite
            postgresql_18
            openssl
          ]
          ++ lib.optionals stdenv.hostPlatform.isDarwin [
            libiconv
            apple-sdk_15
          ];

        # Shared Crane setup for builds and checks
        mkCraneLib =
          pkgs:
          let
            craneLib = (crane.mkLib pkgs).overrideToolchain (mkRustToolchain pkgs);
            src = craneLib.cleanCargoSource ./.;
            commonArgs = {
              inherit src;
              buildInputs = mkBuildInputs pkgs;
              nativeBuildInputs = [ pkgs.pkg-config ];
              strictDeps = true;
            };
            cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          in
          {
            inherit
              craneLib
              src
              commonArgs
              cargoArtifacts
              ;
          };
      in
      {
        inherit inputs;

        systems = [
          "aarch64-darwin"
          "aarch64-linux"
          "x86_64-linux"
        ];

        nixpkgs.config.allowUnfree = true;

        withOverlays = [
          devshell.overlays.default
          rust-overlay.overlays.default
        ];

        package =
          { pkgs, ... }:
          let
            c = mkCraneLib pkgs;
          in
          c.craneLib.buildPackage (
            c.commonArgs
            // {
              inherit (c) cargoArtifacts;
              nativeBuildInputs = c.commonArgs.nativeBuildInputs ++ [ pkgs.installShellFiles ];
              postInstall = ''
                installShellCompletion --cmd curator \
                  --bash <($out/bin/curator completions bash) \
                  --zsh <($out/bin/curator completions zsh) \
                  --fish <($out/bin/curator completions fish)
              '';
              meta.mainProgram = "curator";
            }
          );

        app = pkgs: {
          type = "app";
          program = lib.getExe self.packages.${pkgs.system}.default;
          meta.description = "Curator - repository management tool";
        };

        devShell =
          pkgs:
          let
            rustToolchain = mkRustToolchain pkgs;

            preCommitCheck = git-hooks.lib.${pkgs.system}.run {
              src = ./.;
              package = pkgs.prek;
              hooks = {
                nix-fmt = {
                  enable = true;
                  name = "nix fmt";
                  entry = "nix fmt";
                  pass_filenames = false;
                  stages = [ "pre-commit" ];
                };
                nix-flake-check = {
                  enable = true;
                  name = "nix flake check";
                  entry = "nix flake check";
                  pass_filenames = false;
                  stages = [ "pre-commit" ];
                };
              };
            };
          in
          pkgs.devshell.mkShell {
            name = "curator-dev";

            imports = [ "${devshell}/extra/language/c.nix" ];

            language.c = {
              includes = mkBuildInputs pkgs;
              libraries = mkBuildInputs pkgs;
            };

            packages =
              with pkgs;
              [
                act
                cargo-hack
                cargo-llvm-cov
                cargo-nextest
                cargo-release
                litecli
                nixd
                pkg-config
                rustToolchain
              ]
              ++ mkBuildInputs pkgs;

            env = [
              {
                name = "RUST_SRC_PATH";
                value = "${rustToolchain}/lib/rustlib/src/rust/library";
              }
            ];

            devshell.startup.pre-commit.text = preCommitCheck.shellHook;
          };

        formatter =
          pkgs:
          let
            treefmtConfig = treefmt-nix.lib.evalModule pkgs {
              projectRootFile = "flake.nix";
              programs = {
                deadnix.enable = true;
                mdformat.enable = true;
                nixfmt.enable = true;
                prettier = {
                  enable = true;
                  includes = [ "*.json" ];
                };
                rustfmt = {
                  enable = true;
                  package = mkRustToolchain pkgs;
                };
                shellcheck = {
                  enable = true;
                  includes = [ ".envrc" ];
                };
                shfmt = {
                  enable = true;
                  includes = [ ".envrc" ];
                };
                statix.enable = true;
                stylua.enable = true;
                taplo.enable = true;
                yamlfmt = {
                  enable = true;
                  settings = builtins.fromJSON (
                    builtins.readFile (
                      pkgs.runCommand "yamlfmt-config.json" { nativeBuildInputs = [ pkgs.yj ]; } ''
                        yj -yj < ${./.yamlfmt} > $out
                      ''
                    )
                  );
                };
              };
            };
          in
          treefmt-nix.lib.mkWrapper pkgs (
            treefmtConfig.config
            // {
              build.wrapper = pkgs.writeShellScriptBin "treefmt-nix" ''
                exec ${treefmtConfig.config.build.wrapper}/bin/treefmt --no-cache "$@"
              '';
            }
          );

        checks = {
          curator-clippy =
            pkgs:
            let
              c = mkCraneLib pkgs;
            in
            c.craneLib.cargoClippy (
              c.commonArgs
              // {
                inherit (c) cargoArtifacts;
                cargoClippyExtraArgs = "--all-targets -- --deny warnings";
              }
            );

          curator-nextest =
            pkgs:
            let
              c = mkCraneLib pkgs;
            in
            c.craneLib.cargoNextest (
              c.commonArgs
              // {
                inherit (c) cargoArtifacts;
                nativeBuildInputs = c.commonArgs.nativeBuildInputs ++ [ pkgs.cargo-nextest ];
              }
            );
        };
      }
    );
}
