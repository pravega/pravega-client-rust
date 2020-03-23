//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

const EPOCH_DELIMITER: &str = ".#epoch.";
const TRANSACTION_DELIMITER: &str = "#transaction.";
const TRANSACTION_PART_LENGTH: i32 = 16;
const TRANSACTION_ID_LENGTH: i32 = 2 * TRANSACTION_PART_LENGTH;

pub struct NameUtils {}

impl NameUtils {
    pub fn get_segment_number(segment_id: i64) -> i32 {
        segment_id as i32
    }

    pub fn get_epoch(segment_id: i64) -> i32 {
        (segment_id >> 32) as i32
    }

    pub fn is_transaction_segment(stream_segment_name: &str) -> bool {
        // Check to see if the given name is a properly formatted Transaction.
        let end_of_stream_name_pos = stream_segment_name.rfind(TRANSACTION_DELIMITER);
        match end_of_stream_name_pos {
            Some(pos) => {
                pos + TRANSACTION_DELIMITER.len() + TRANSACTION_ID_LENGTH as usize
                    <= stream_segment_name.len()
            }
            None => false,
        }
    }

    pub fn get_parent_stream_segment_name(transaction_name: &str) -> &str {
        // Check to see if the given name is a properly formatted Transaction.
        let end_of_stream_name_pos = transaction_name.rfind(TRANSACTION_DELIMITER);
        match end_of_stream_name_pos {
            Some(pos) => {
                if pos + TRANSACTION_DELIMITER.len() + TRANSACTION_ID_LENGTH as usize > transaction_name.len()
                {
                    panic!("name is not legal");
                } else {
                    &transaction_name[..pos]
                }
            }
            None => {
                panic!("name is not legal");
            }
        }
    }

    pub fn extract_segment_tokens(qualified_name: String) -> Vec<String> {
        assert!(!qualified_name.is_empty());
        let original_segment_name = if NameUtils::is_transaction_segment(&qualified_name) {
            String::from(NameUtils::get_parent_stream_segment_name(&qualified_name))
        } else {
            qualified_name
        };

        let mut ret_val = vec![];
        let tokens: Vec<&str> = original_segment_name.split('/').collect();
        let segment_id_index = if tokens.len() == 2 { 1 } else { 2 };

        let segment_id = if tokens[segment_id_index].contains(EPOCH_DELIMITER) {
            let segment_id_string = String::from(tokens[segment_id_index]);
            let segment_id_tokens: Vec<&str> = segment_id_string.split(EPOCH_DELIMITER).collect();
            NameUtils::compute_segment_id(
                segment_id_tokens[0].parse::<i32>().expect("parse to i32"),
                segment_id_tokens[1].parse::<i32>().expect("parse to i32"),
            )
        } else {
            NameUtils::compute_segment_id(tokens[segment_id_index].parse::<i32>().expect("parse to i32"), 0)
        };
        // no secondary delimiter, set the secondary id to 0 for segment id computation
        ret_val.push(String::from(tokens[0]));
        if tokens.len() == 3 {
            ret_val.push(String::from(tokens[1]));
        }
        ret_val.push(segment_id.to_string());

        ret_val
    }

    pub fn compute_segment_id(segment_number: i32, epoch: i32) -> i64 {
        assert!(segment_number >= 0);
        assert!(epoch >= 0);
        (i64::from(epoch) << 32) + i64::from(segment_number)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_segment_id() {
        let segment_number = i32::max_value();
        let epoch = 1;
        let result = i64::from_str_radix("000000017fffffff", 16).expect("i64");
        assert_eq!(NameUtils::compute_segment_id(segment_number, epoch), result);
    }
}
