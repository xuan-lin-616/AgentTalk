fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [version] if version == "--version" => println!("codex-cli 1.2.3"),
        [subcommand, help] if subcommand == "app-server" && help == "--help" => {
            println!("Usage: codex app-server [OPTIONS]")
        }
        _ => std::process::exit(2),
    }
}
