use std::env;

fn main() {
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());

    let mut features = Vec::new();

    #[cfg(feature = "parquet")]
    features.push("parquet");

    if features.is_empty() {
        features.push("none");
    }

    let features_str = features.join(", ");

    println!("cargo:rustc-env=BUILD_TARGET={}", target);
    println!("cargo:rustc-env=BUILD_PROFILE={}", profile);
    println!("cargo:rustc-env=BUILD_FEATURES={}", features_str);
}
