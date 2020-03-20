//
// Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//

//const EPOCH_DELIMITER: &str = ".#epoch.";
//
//pub struct NameUtils {}
//
//impl NameUtils {
//
//    pub fn get_qualified_stream_segment_name(scope: &str, stream_name: &str, segment_id: i64) -> String {
//        let segment_number = NameUtils::get_segment_number(segment_id);
//        let epoch = NameUtils::get_epoch(segment_id);
//        let scoped_stream_name = NameUtils:: get_scoped_stream_name_internal(scope, stream_name);
//        format!("{}{}{}{}{}", scoped_stream_name, "/", segment_number, EPOCH_DELIMITER, epoch)
//    }
//
//    pub fn get_segment_number(segment_id: i64) -> i32 {
//        segment_id as i32
//    }
//
//    pub fn get_epoch(segment_id: i64) -> i32 {
//        (segment_id >> 32) as i32
//    }
//
//    fn get_scoped_stream_name_internal(scope: &str, stream_name: &str) -> String {
//        format!("{}{}{}", scope, "/", stream_name)
//    }
//}
