//! Solar position calculator CLI entry point.

fn main() {
    std::process::exit(sunce::run(std::env::args().collect()));
}
