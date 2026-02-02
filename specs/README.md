# OpenAPI Specs

## Generation flow

- `specs/gitlab-openapi3.yaml` is generated from GitLab's Swagger 2.0 spec via
  `swagger2openapi` and then patched with `specs/gitlab-openapi3-patch.yaml`.
- `specs/gitlab-openapi3-patch.yaml` holds overrides/additions that are merged
  into the converted spec (including shared schemas for patched endpoints).
- `specs/gitlab-openapi3-trimmed.yaml` is a curated subset used by
  `crates/curator/build.rs`; keep it in sync when updating shared schemas or
  request/response docs.

See `flake.nix` (search for `mkGitlabOpenapiSpec`) for the exact conversion
pipeline used in Nix builds.
