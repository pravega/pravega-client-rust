use flate2::read::GzDecoder;
use log::info;
use std::fs::File;
use std::io;
use std::path::Path;
use tar::Archive;

const LIBRARY: &str = "pravega";
const VERSION: &str = "0.8.0-2593.6e882cf-SNAPSHOT";
const TAG: &str = "delta";
const BASE: &str = "./";

fn main() {
    // first check if the Pravega directory already exists.
    if check_exist() {
        info!("Returning early because {} was already found", LIBRARY);
        return;
    }
    install_prebuilt();
}

fn check_exist() -> bool {
    let path = Path::new(BASE).join(LIBRARY);
    if path.exists() {
        return true;
    }
    false
}

fn remove_suffix(value: &mut String, suffix: &str) {
    if value.ends_with(suffix) {
        let n = value.len();
        value.truncate(n - suffix.len());
    }
}

/// Downloads and unpacks a prebuilt binary. Only works for certain platforms.
fn install_prebuilt() {
    let url = format!(
        "https://github.com/Tristan1900/pravega/releases/download/{}/pravega-{}.tgz",
        TAG, VERSION
    );
    let short_file_name = url.split('/').last().unwrap();
    let mut base_name = short_file_name.to_string();
    remove_suffix(&mut base_name, ".tgz");

    let file_name = Path::new(BASE).join(short_file_name);

    // check the tarball and download the tarball.
    if !file_name.exists() {
        let mut f = File::create(&file_name).unwrap();
        let mut resp = reqwest::blocking::get(&url).unwrap();
        io::copy(&mut resp, &mut f).unwrap();
    }

    // Extract the Pravega standalone.
    let unpacked_dir = ".";
    let tar_gz = File::open(file_name).unwrap();
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    archive.unpack(unpacked_dir).unwrap();

    //Rename the directory to pravega
    let old_path = Path::new(BASE).join(base_name);
    let new_path = Path::new(BASE).join(LIBRARY);
    std::fs::rename(old_path, new_path).unwrap();
}
