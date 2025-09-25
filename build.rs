use std::env;

fn main() {
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "unknown".to_string());
    let build_date = chrono::Utc::now()
        .format("%Y-%m-%d %H:%M:%S UTC")
        .to_string();

    // Detect enabled features
    let mut features = Vec::new();

    #[cfg(feature = "parquet")]
    features.push("parquet");

    #[cfg(feature = "minimal")]
    features.push("minimal");

    if features.is_empty() {
        features.push("default");
    }

    let features_str = features.join(", ");

    println!("cargo:rustc-env=BUILD_TARGET={}", target);
    println!("cargo:rustc-env=BUILD_PROFILE={}", profile);
    println!("cargo:rustc-env=BUILD_DATE={}", build_date);
    println!("cargo:rustc-env=BUILD_FEATURES={}", features_str);
}
