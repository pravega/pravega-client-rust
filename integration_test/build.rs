//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

use flate2::read::GzDecoder;
use std::fs::File;
use std::io;
use std::path::Path;
use tar::Archive;
use tracing::info;

const LIBRARY: &str = "pravega";
const VERSION: &str = "0.10.1";
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
        "https://github.com/pravega/pravega/releases/download/v{}/pravega-{}.tgz",
        VERSION, VERSION
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
    dbg!(&tar_gz);
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);
    archive.unpack(unpacked_dir).unwrap();

    //Rename the directory to pravega
    let old_path = Path::new(BASE).join(base_name);
    let new_path = Path::new(BASE).join(LIBRARY);
    std::fs::rename(old_path, new_path).unwrap();
}
