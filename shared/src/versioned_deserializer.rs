//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
use bincode2::Config;
use bincode2::LengthOption;
use lazy_static::*;
use serde::{Deserialize, Serialize};

lazy_static! {
    static ref CONFIG: Config = {
        let mut config = bincode2::config();
        config.big_endian();
        config.limit(0x007f_ffff);
        config.array_length(LengthOption::U32);
        config.string_length(LengthOption::U16);
        config
    };
}

struct VersionedDeserializer {

}

impl VersionedDeserializer {
    fn deserializer() {

    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct FooV1Rv1 {
        name: String,
        age: i32,
        games: HashMap<String, String>,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    enum V1Rv1 {
        V1(FooV1Rv1),
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct FooV1Rv2 {
        name: String,
        age: i32,
        games: HashMap<String, String>,
        books: HashMap<String, String>,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    enum V1Rv2 {
        V1(FooV1Rv2),
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    struct FooV2Rv1 {
        name: String,
        age: i32,
        hobby: String,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
    enum V2Rv1{
        V1(FooV1Rv2),
        V2(FooV2Rv1),
    }

    #[test]
    fn test_revision() {
        let mut games = HashMap::new();
        games.insert("FPS".to_string(), "COD".to_string());
        games.insert("RPG".to_string(), "Witcher".to_string());

        let mut books = HashMap::new();
        books.insert("favorite".to_string(), "Sherlock Holmes".to_string());

        let rv2 = FooV1Rv2 { name: "Mike".to_string(), age: 16, games, books};
        let encoded_v2 = CONFIG.serialize(&rv2).expect("serialize");


        let decoded_v1: FooV1Rv1 = CONFIG.deserialize(&encoded_v2[..]).expect("deserialize");
        let decoded_v2: FooV1Rv2 = CONFIG.deserialize(&encoded_v2[..]).expect("deserialize");
        println!("foorv1 {:?}", decoded_v1);
        println!("foorv2 {:?}", decoded_v2);

    }

    #[test]
    fn test_version() {
        let mut games = HashMap::new();
        games.insert("FPS".to_string(), "COD".to_string());
        games.insert("RPG".to_string(), "Witcher".to_string());

        let mut books = HashMap::new();
        books.insert("favorite".to_string(), "Sherlock Holmes".to_string());

        let v1 = FooV1Rv2 { name: "Mike".to_string(), age: 16, games, books};
        let v2 = FooV2Rv1 {name: "Mike".to_string(), age: 16, hobby: "video games".to_string()};

        let enum1 = V1Rv2::V1(v1);
        let enum2 = V2Rv1::V2(v2);

        let encoded_v1 = CONFIG.serialize(&enum1).expect("serialize");
        let encoded_v2 = CONFIG.serialize(&enum2).expect("serialize");


        let decoded_v1rv1: V1Rv1 = CONFIG.deserialize(&encoded_v1[..]).expect("deserialize");
        let decoded_v1rv2: V1Rv2 = CONFIG.deserialize(&encoded_v1[..]).expect("deserialize");
        let decoded_v2rv1: V2Rv1 = CONFIG.deserialize(&encoded_v1[..]).expect("deserialize");

        println!("foorv1 {:?}", decoded_v1rv1);
        println!("foorv1 {:?}", decoded_v1rv2);
        println!("foorv2 {:?}", decoded_v2rv1);
    }
}