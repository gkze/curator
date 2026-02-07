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

        # swagger2openapi: Convert Swagger 2.0 specs to OpenAPI 3.0
        mkSwagger2openapi =
          pkgs:
          pkgs.buildNpmPackage {
            pname = "swagger2openapi";
            version = "7.0.8";

            src = pkgs.fetchFromGitHub {
              owner = "Mermade";
              repo = "oas-kit";
              rev = "b1bba3fc5007e96a991bf2a015cf0534ac36b88b";
              hash = "sha256-7uYfeq8TeeeshA86gFXGTK+EyTMw46ZXGVlMgugusB0=";
            };

            sourceRoot = "source/packages/swagger2openapi";

            postPatch = ''
              cp ${./nix/swagger2openapi/package-lock.json} package-lock.json
            '';

            npmDepsHash = "sha256-Qh+JPtGht+z3ooDw9Bw2NmeI4IvIErCaYcwl87XE96k=";
            dontNpmBuild = true;
          };

        # GitLab OpenAPI 3.0 spec: fetched from GitLab's auto-generated Swagger 2.0
        # spec, converted to OpenAPI 3.0, and patched with missing endpoints.
        mkGitlabOpenapiSpec =
          pkgs:
          let
            swagger2openapi = mkSwagger2openapi pkgs;
            gitlabSwaggerSpec = pkgs.fetchurl {
              url = "https://gitlab.com/gitlab-org/gitlab/-/raw/0b27f4409b76afcffa55363f213b4266fd7693e4/doc/api/openapi/openapi_v2.yaml";
              hash = "sha256:0wp0gz84l20l18955rw1cgsijagm48x7jz4ysj059zzh1xvzj28y";
            };
          in
          pkgs.runCommand "gitlab-openapi3-spec"
            {
              nativeBuildInputs = [
                swagger2openapi
                pkgs.yq-go
              ];
            }
            ''
              # Convert Swagger 2.0 → OpenAPI 3.0
              swagger2openapi ${gitlabSwaggerSpec} -o converted.yaml -y

              # Patch in missing endpoints (e.g., GET /api/v4/user)
              yq eval-all 'select(fileIndex == 0) * select(fileIndex == 1)' \
                converted.yaml ${./specs/gitlab-openapi3-patch.yaml} > $out
            '';

        # Shared Crane setup for builds and checks
        mkCraneLib =
          pkgs:
          let
            craneLib = (crane.mkLib pkgs).overrideToolchain (mkRustToolchain pkgs);
            src = lib.cleanSourceWith {
              src = ./.;
              filter =
                path: type:
                (craneLib.filterCargoSources path type)
                || (lib.hasSuffix ".yaml" path && lib.hasInfix "/specs/" path);
            };
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

                # Generate and install man pages (main command + all subcommands)
                mkdir -p man-pages
                $out/bin/curator man --output man-pages
                installManPage man-pages/*.1
              '';
              meta.mainProgram = "curator";
            }
          );

        packages = {
          swagger2openapi = mkSwagger2openapi;
          gitlab-openapi-spec = mkGitlabOpenapiSpec;
        };

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
                cargo-dist-generate-check = {
                  enable = true;
                  name = "cargo dist generate --check";
                  entry = "cargo dist generate --mode ci --check";
                  pass_filenames = false;
                  stages = [ "pre-commit" ];
                };
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
                cargo-dist
                cargo-llvm-cov
                cargo-nextest
                cargo-release
                litecli
                nixd
                pkg-config
                rustToolchain
                yq-go
              ]
              ++ [
                self.packages.${pkgs.system}.default
                (mkSwagger2openapi pkgs)
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
