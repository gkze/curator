use std::path::PathBuf;

use clap::CommandFactory;

use crate::Cli;

fn cli_command() -> clap::Command {
    Cli::command()
}

fn completion_script(shell: clap_complete::Shell) -> Vec<u8> {
    let mut cmd = cli_command();
    let mut out = Vec::new();
    clap_complete::generate(shell, &mut cmd, "curator", &mut out);
    out
}

fn main_man_page() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let man = clap_mangen::Man::new(cli_command());
    let mut out = Vec::new();
    man.render(&mut out)?;
    Ok(out)
}

pub(crate) fn handle_completions(
    shell: clap_complete::Shell,
) -> Result<(), Box<dyn std::error::Error>> {
    let out = completion_script(shell);
    use std::io::Write;
    std::io::stdout().write_all(&out)?;
    Ok(())
}

pub(crate) fn handle_man(output: Option<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    match output {
        Some(dir) => {
            // Generate all man pages (main + subcommands) to directory
            std::fs::create_dir_all(&dir)?;
            clap_mangen::generate_to(cli_command(), &dir)?;
            println!("Generated man pages in: {}", dir.display());
        }
        None => {
            // Print main man page to stdout
            let out = main_man_page()?;
            use std::io::Write;
            std::io::stdout().write_all(&out)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn completion_script_contains_binary_name() {
        let script = completion_script(clap_complete::Shell::Bash);
        let script = String::from_utf8(script).expect("completion output should be UTF-8");
        assert!(script.contains("curator"));
    }

    #[test]
    fn main_man_page_contains_expected_title() {
        let page = main_man_page().expect("man rendering should succeed");
        let page = String::from_utf8(page).expect("man output should be UTF-8");
        assert!(page.to_lowercase().contains(".th curator"));
    }

    #[test]
    fn handle_man_writes_files_when_output_directory_is_provided() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("curator-man-test-{nonce}"));

        handle_man(Some(dir.clone())).expect("man page generation should succeed");

        let entries = std::fs::read_dir(&dir)
            .expect("output directory should exist")
            .count();
        assert!(entries > 0, "man page generation should produce files");

        std::fs::remove_dir_all(&dir).expect("test output directory should be removable");
    }
}
