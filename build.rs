use clap_complete::{generate_to, shells::*};
use std::env;
use std::io::Error;

include!("src/cli.rs");

fn main() -> Result<(), Error> {
    let outdir = match env::var_os("OUT_DIR") {
        None => return Ok(()),
        Some(outdir) => outdir,
    };

    let mut cmd = build_cli();
    let path = generate_to(Bash, &mut cmd, "sunce", &outdir)?;
    println!("cargo:warning=Generated Bash completion file: {:?}", path);

    let path = generate_to(Zsh, &mut cmd, "sunce", &outdir)?;
    println!("cargo:warning=Generated Zsh completion file: {:?}", path);

    let path = generate_to(Fish, &mut cmd, "sunce", &outdir)?;
    println!("cargo:warning=Generated Fish completion file: {:?}", path);

    let path = generate_to(PowerShell, &mut cmd, "sunce", &outdir)?;
    println!(
        "cargo:warning=Generated PowerShell completion file: {:?}",
        path
    );

    let path = generate_to(Elvish, &mut cmd, "sunce", &outdir)?;
    println!("cargo:warning=Generated Elvish completion file: {:?}", path);

    Ok(())
}
