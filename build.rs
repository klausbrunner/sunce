use std::env;

fn main() {
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());

    #[cfg(feature = "parquet")]
    let features = "parquet";
    #[cfg(not(feature = "parquet"))]
    let features = "none";

    println!("cargo:rustc-env=BUILD_TARGET={}", target);
    println!("cargo:rustc-env=BUILD_PROFILE={}", profile);
    println!("cargo:rustc-env=BUILD_FEATURES={}", features);
}
