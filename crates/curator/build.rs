use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let spec_path = Path::new("../../specs/gitlab-openapi3-trimmed.yaml");

    // Only rebuild when the spec changes
    println!("cargo::rerun-if-changed={}", spec_path.display());

    let spec_contents = fs::read_to_string(spec_path).unwrap_or_else(|e| {
        panic!(
            "Failed to read GitLab OpenAPI spec at {}: {e}",
            spec_path.display()
        )
    });

    // Parse YAML to JSON (Progenitor expects JSON)
    let spec_value: serde_json::Value = serde_yaml_ng::from_str(&spec_contents)
        .unwrap_or_else(|e| panic!("Failed to parse GitLab OpenAPI spec YAML: {e}"));

    let spec = serde_json::from_value(spec_value)
        .unwrap_or_else(|e| panic!("Failed to parse GitLab OpenAPI spec as OpenAPI: {e}"));

    let mut generator = progenitor::Generator::default();

    let tokens = generator
        .generate_tokens(&spec)
        .unwrap_or_else(|e| panic!("Failed to generate GitLab client: {e}"));

    let ast = syn::parse2(tokens)
        .unwrap_or_else(|e| panic!("Failed to parse generated GitLab client tokens: {e}"));

    let content = prettyplease::unparse(&ast);

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR not set");
    let out_path = Path::new(&out_dir).join("gitlab_generated.rs");
    fs::write(&out_path, content)
        .unwrap_or_else(|e| panic!("Failed to write generated GitLab client: {e}"));
}
