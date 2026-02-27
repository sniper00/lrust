// extern crate cc;
use std::env;
// use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=lualib-src");

    if cfg!(target_os = "windows") {
        let profile = env::var("PROFILE").unwrap_or_else(|_| "release".to_string());
        let profile_dir = if profile == "release" { "release" } else { "debug" };
        println!(r"cargo:rustc-link-search=native=../../target/{}", profile_dir);
        println!("cargo:rustc-link-lib=dylib=moon");
        println!("cargo:rustc-link-lib=moon");
    } else if cfg!(target_os = "macos") {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}
