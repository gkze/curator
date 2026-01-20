use std::path::PathBuf;

use clap::CommandFactory;

use crate::Cli;

pub(crate) fn handle_completions(
    shell: clap_complete::Shell,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Cli::command();
    clap_complete::generate(shell, &mut cmd, "curator", &mut std::io::stdout());
    Ok(())
}

pub(crate) fn handle_man(output: Option<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
    let cmd = Cli::command();

    match output {
        Some(dir) => {
            // Generate all man pages (main + subcommands) to directory
            std::fs::create_dir_all(&dir)?;
            clap_mangen::generate_to(cmd, &dir)?;
            println!("Generated man pages in: {}", dir.display());
        }
        None => {
            // Print main man page to stdout
            let man = clap_mangen::Man::new(cmd);
            man.render(&mut std::io::stdout())?;
        }
    }

    Ok(())
}
