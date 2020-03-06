/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
use super::*;

#[test]
fn test_valid_segments() {
    let segment_1 = create_segment(1);

    let segment_0 = create_segment(0);

    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    segment_map.insert(
        OrderedFloat(0.5),
        SegmentWithRange::new(segment_0.clone(), OrderedFloat(0.0), OrderedFloat(0.5)),
    );
    segment_map.insert(
        OrderedFloat(1.0),
        SegmentWithRange::new(segment_1.clone(), OrderedFloat(0.5), OrderedFloat(1.0)),
    );
    let s = StreamSegments::new(segment_map);
    assert_eq!(s.key_segment_map.len(), 2);
    assert_eq!(segment_1.clone(), s.get_segment(0.75));
    assert_eq!(segment_1.clone(), s.get_segment(1.0));
    assert_eq!(segment_0.clone(), s.get_segment(0.5));
    assert_eq!(segment_0.clone(), s.get_segment(0.4));
    assert_eq!(segment_0.clone(), s.get_segment(0.499));
}

#[test]
#[should_panic] // invalid entry in stream segments.
fn test_invalid_streamsegments() {
    let segment_0 = create_segment(0);

    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    segment_map.insert(
        OrderedFloat(0.0),
        SegmentWithRange::new(segment_0, OrderedFloat(0.0), OrderedFloat(0.0)),
    );
    let _s = StreamSegments::new(segment_map);
}

#[test]
#[should_panic] // invalid entry in stream segments.
fn test_invalid_segments_max_range() {
    let segment_0 = create_segment(0);

    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    segment_map.insert(
        OrderedFloat(2.0),
        SegmentWithRange::new(segment_0, OrderedFloat(0.0), OrderedFloat(2.0)),
    );
    let _s = StreamSegments::new(segment_map);
}

#[test]
#[should_panic] // invalid entry in stream segments.
fn test_invalid_segments_missing_range() {
    let segment_0 = create_segment(0);

    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    segment_map.insert(
        OrderedFloat(0.5),
        SegmentWithRange::new(segment_0, OrderedFloat(0.0), OrderedFloat(0.5)),
    );
    let _s = StreamSegments::new(segment_map);
}

#[test]
fn test_get_key_invalid_key_greaterthanone() {
    let segment_0 = create_segment(0);

    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    segment_map.insert(
        OrderedFloat(1.0),
        SegmentWithRange::new(segment_0, OrderedFloat(0.0), OrderedFloat(1.0)),
    );
    let s = StreamSegments::new(segment_map);
    let result = std::panic::catch_unwind(|| s.get_segment(1.1));
    assert!(result.is_err());
    let result = std::panic::catch_unwind(|| s.get_segment(-0.1));
    assert!(result.is_err());
}

#[test]
fn test_replace_range_for_split() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.5);
    add_segment_entry(&mut segment_map, 1, 0.5, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 2, 0.0, 0.25, [0].to_vec());
    add_replacement_segment(&mut segment_map, 3, 0.25, 0.5, [0].to_vec());
    let successor_for_segment_0 = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(0), &successor_for_segment_0)
        .unwrap();

    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 4, 0.5, 0.75, [1].to_vec());
    add_replacement_segment(&mut segment_map, 5, 0.75, 1.0, [1].to_vec());

    let updated_range = updated_range
        .apply_replacement_range(
            &Segment::new(1),
            &StreamSegmentsWithPredecessors::new(segment_map.into()),
        )
        .unwrap();

    println!("updated_range {:?}", updated_range);
    assert_eq!(create_segment(2), updated_range.get_segment(0.2));
    assert_eq!(create_segment(3), updated_range.get_segment(0.4));
    assert_eq!(create_segment(4), updated_range.get_segment(0.6));
    assert_eq!(create_segment(5), updated_range.get_segment(0.8));
}

#[test]
fn test_replace_range_for_merge() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.25);
    add_segment_entry(&mut segment_map, 1, 0.25, 0.5);
    add_segment_entry(&mut segment_map, 2, 0.5, 0.75);
    add_segment_entry(&mut segment_map, 3, 0.75, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    //simulate successors for segment 0
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.0, 0.5, [0, 1].to_vec());
    let successor_for_segment_0_1 = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(0), &successor_for_segment_0_1)
        .unwrap();

    assert_eq!(create_segment(41), updated_range.get_segment(0.2));
    assert_eq!(create_segment(1), updated_range.get_segment(0.4));
    assert_eq!(create_segment(2), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 1
    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(1), &successor_for_segment_0_1)
        .unwrap();

    assert_eq!(create_segment(41), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(2), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    // simulate successors for segment 2
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 51, 0.5, 1.0, [2, 3].to_vec());
    let successor_for_segment_2_3 = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(2), &successor_for_segment_2_3)
        .unwrap();

    assert_eq!(create_segment(41), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(51), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    // simulate successors for segment 3
    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(3), &successor_for_segment_2_3)
        .unwrap();
    assert_eq!(create_segment(41), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(51), updated_range.get_segment(0.6));
    assert_eq!(create_segment(51), updated_range.get_segment(0.8));
}

#[test]
fn test_replace_range_for_merge_multiple() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.25);
    add_segment_entry(&mut segment_map, 1, 0.25, 0.5);
    add_segment_entry(&mut segment_map, 2, 0.5, 0.75);
    add_segment_entry(&mut segment_map, 3, 0.75, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    // all segments merged to a single segment.
    //simulate successors for segment 0
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.0, 1.0, [3, 1, 0, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(0), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(41), updated_range.get_segment(0.2));
    assert_eq!(create_segment(1), updated_range.get_segment(0.4));
    assert_eq!(create_segment(2), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));
}

#[test]
fn test_replace_range_multiple_order() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.33);
    add_segment_entry(&mut segment_map, 1, 0.33, 0.66);
    add_segment_entry(&mut segment_map, 2, 0.66, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    // all segments merged to a single segment.
    //simulate successors for segment 2
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 31, 0.0, 1.0, [1, 0, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(2), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(1), updated_range.get_segment(0.4));
    assert_eq!(create_segment(31), updated_range.get_segment(0.8));

    // simulate successors for segment 0
    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(0), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(31), updated_range.get_segment(0.2));
    assert_eq!(create_segment(1), updated_range.get_segment(0.4));
    assert_eq!(create_segment(31), updated_range.get_segment(0.8));

    // simulate successors for segment 1
    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(1), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(31), updated_range.get_segment(0.2));
    assert_eq!(create_segment(31), updated_range.get_segment(0.4));
    assert_eq!(create_segment(31), updated_range.get_segment(0.8));
}

/**
Segment Map used by the test.
       ^
       |
   1.0 +---------+---------+-------->
       |         |         |
       |    2    |         |    5
       |         |    3    |
   0.66+---------|         +-------->
       |         |         |
       |    1    |---------+    6
       |         |         |
   0.33+---------|         |-------->
       |         |    4    |
       |    0    |         |    7
       |         |         |
       +-------------------+-------->
       +         1         2
 */
#[test]
fn test_replace_range_uneven_range() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.33);
    add_segment_entry(&mut segment_map, 1, 0.33, 0.66);
    add_segment_entry(&mut segment_map, 2, 0.66, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    assert_eq!(create_segment(0), orig_segment_map.get_segment(0.2));
    assert_eq!(create_segment(1), orig_segment_map.get_segment(0.4));
    assert_eq!(create_segment(1), orig_segment_map.get_segment(0.6));
    assert_eq!(create_segment(2), orig_segment_map.get_segment(0.8));

    //simulate successors for segment 1
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 31, 0.5, 1.0, [1, 2].to_vec());
    add_replacement_segment(&mut segment_map, 41, 0.0, 0.5, [1, 0].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(1), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(31), updated_range.get_segment(0.6));
    assert_eq!(create_segment(2), updated_range.get_segment(0.8));

    //simulate successor for segment 0
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.0, 0.5, [1, 0].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());
    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(0), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(41), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(31), updated_range.get_segment(0.6));
    assert_eq!(create_segment(2), updated_range.get_segment(0.8));

    //simulate successor for segment 2
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 31, 0.5, 1.0, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());
    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(2), &successor_for_segment)
        .unwrap();
    assert_eq!(create_segment(41), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(31), updated_range.get_segment(0.6));
    assert_eq!(create_segment(31), updated_range.get_segment(0.8));

    //simulate successor for segment 31
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.66, 1.0, [31].to_vec());
    add_replacement_segment(&mut segment_map, 62, 0.33, 0.66, [31, 41].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(31), &successor_for_segment)
        .unwrap();
    assert_eq!(create_segment(41), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(52), updated_range.get_segment(0.8));

    //simulate successor for segment 41
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 72, 0.0, 0.33, [41].to_vec());
    add_replacement_segment(&mut segment_map, 62, 0.33, 0.66, [31, 41].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(41), &successor_for_segment)
        .unwrap();
    assert_eq!(create_segment(72), updated_range.get_segment(0.2));
    assert_eq!(create_segment(62), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(52), updated_range.get_segment(0.8));
}

/**
Segment Map used by the test.
       ^
       |
   1.0 +---------------------------->
       |
       |    3
       |
   0.75+---------+---------+-------->
       |         |         |
       |    2    |         |
       |         |         |
   0.50+---------|    4    |
       |         |         |
       |    1    |         |    5
       |         |         |
   0.25+---------+---------+
       |                   |
       |    0              |
       |                   |
   0.0 +-------------------+--------->
       +         1         2
 */
#[test]
fn test_replace_range_double_merge_low() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.25);
    add_segment_entry(&mut segment_map, 1, 0.25, 0.5);
    add_segment_entry(&mut segment_map, 2, 0.5, 0.75);
    add_segment_entry(&mut segment_map, 3, 0.75, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    //simulate successors for segment 1
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.25, 0.75, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(1), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(2), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 2
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.25, 0.75, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(2), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(41), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 41
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.0, 0.75, [0, 41].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(41), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(52), updated_range.get_segment(0.4));
    assert_eq!(create_segment(52), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 0
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.0, 0.75, [0, 41].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(0), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(52), updated_range.get_segment(0.2));
    assert_eq!(create_segment(52), updated_range.get_segment(0.4));
    assert_eq!(create_segment(52), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));
}

/**
Segment Map used by the test.
       ^
       |
   1.0 +---------------------------->
       |                   |
       |    3              |
       |                   |
   0.75+---------+---------+
       |         |         |
       |    2    |         |    5
       |         |         |
   0.50+---------|    4    |
       |         |         |
       |    1    |         |
       |         |         |
   0.25+---------+---------+-------->
       |
       |    0
       |
   0.0 +-------------------+--------->
       +         1         2
 */
#[test]
fn test_replace_range_double_merge_high() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.25);
    add_segment_entry(&mut segment_map, 1, 0.25, 0.5);
    add_segment_entry(&mut segment_map, 2, 0.5, 0.75);
    add_segment_entry(&mut segment_map, 3, 0.75, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    //simulate successors for segment 1
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.25, 0.75, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(1), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(2), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 2
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.25, 0.75, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(2), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(41), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 41
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.25, 1.0, [3, 41].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(41), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(52), updated_range.get_segment(0.4));
    assert_eq!(create_segment(52), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 3
    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(3), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(52), updated_range.get_segment(0.4));
    assert_eq!(create_segment(52), updated_range.get_segment(0.6));
    assert_eq!(create_segment(52), updated_range.get_segment(0.8));
}

/**
Segment Map used by the test.
       ^
       |
   1.0 +----------------------------->
       |                   |
       |    3              |
       |                   |
   0.75+---------+---------+
       |         |         |
       |    2    |         |    5
       |         |         |
   0.50+---------|    4    +--------->
       |         |         |
       |    1    |         |
       |         |         |
   0.25+---------+---------+    6
       |                   |
       |    0              |
       |                   |
   0.0 +-------------------+--------->
       +         1         2
 */
#[test]
fn test_replace_range_double_merge() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.25);
    add_segment_entry(&mut segment_map, 1, 0.25, 0.5);
    add_segment_entry(&mut segment_map, 2, 0.5, 0.75);
    add_segment_entry(&mut segment_map, 3, 0.75, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    //simulate successors for segment 1
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.25, 0.75, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(1), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(2), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 2
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.25, 0.75, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(2), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(41), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 41
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.5, 1.0, [41, 3].to_vec());
    add_replacement_segment(&mut segment_map, 62, 0.0, 0.5, [41, 0].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(41), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(62), updated_range.get_segment(0.4));
    assert_eq!(create_segment(52), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));
}

/**
Segment Map used by the test.
       ^
       |
   1.0 +---------------------------->
       |
       |    3
       |
   0.75+---------+---------+-------->
       |         |         |
       |    2    |         |
       |         |         |
   0.50+---------|    4    |
       |         |         |
       |    1    |         |    5
       |         |         |
   0.25+---------+---------+
       |                   |
       |    0              |
       |                   |
   0.0 +-------------------+--------->
       +         1         2
 */
#[test]
fn test_replace_range_double_merge_out_of_order() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.25);
    add_segment_entry(&mut segment_map, 1, 0.25, 0.5);
    add_segment_entry(&mut segment_map, 2, 0.5, 0.75);
    add_segment_entry(&mut segment_map, 3, 0.75, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    //simulate successors for segment 1
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.25, 0.75, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(1), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(2), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 41
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.0, 0.75, [41, 0].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(41), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(52), updated_range.get_segment(0.4));
    assert_eq!(create_segment(2), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 0
    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(0), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(52), updated_range.get_segment(0.2));
    assert_eq!(create_segment(52), updated_range.get_segment(0.4));
    assert_eq!(create_segment(2), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 2
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.25, 0.75, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());
    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(2), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(52), updated_range.get_segment(0.2));
    assert_eq!(create_segment(52), updated_range.get_segment(0.4));
    assert_eq!(create_segment(41), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));

    //simulate successors for segment 41
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.0, 0.75, [41, 0].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(41), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(52), updated_range.get_segment(0.2));
    assert_eq!(create_segment(52), updated_range.get_segment(0.4));
    assert_eq!(create_segment(52), updated_range.get_segment(0.6));
    assert_eq!(create_segment(3), updated_range.get_segment(0.8));
}

/**
*
      ^
      |
  1.0 +---------+---------+-------->
      |         |         |
      |    2    |         |    5
      |         |    3    |
  0.66+---------|         +-------->
      |         |         |
      |    1    |---------+    6
      |         |         |
  0.33+---------|         |-------->
      |         |    4    |
      |    0    |         |    7
      |         |         |
      +-------------------+-------->
      +         1         2
*/
#[test]
fn test_replace_range_uneven_out_of_order() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.33);
    add_segment_entry(&mut segment_map, 1, 0.33, 0.66);
    add_segment_entry(&mut segment_map, 2, 0.66, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    //simulate successors for segment 1
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.0, 0.5, [1, 0].to_vec());
    add_replacement_segment(&mut segment_map, 31, 0.5, 1.0, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(1), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(31), updated_range.get_segment(0.6));
    assert_eq!(create_segment(2), updated_range.get_segment(0.8));

    //simulate successors for segment 3
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 62, 0.33, 0.66, [31, 41].to_vec());
    add_replacement_segment(&mut segment_map, 52, 0.66, 1.0, [31].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(31), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(2), updated_range.get_segment(0.8));

    //simulate successors for segment 0
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.0, 0.5, [0, 1].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(0), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(41), updated_range.get_segment(0.2));
    assert_eq!(create_segment(41), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(2), updated_range.get_segment(0.8));

    //simulate successors for segment 41
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 62, 0.33, 0.66, [31, 41].to_vec());
    add_replacement_segment(&mut segment_map, 72, 0.0, 0.33, [41].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(41), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(72), updated_range.get_segment(0.2));
    assert_eq!(create_segment(62), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(2), updated_range.get_segment(0.8));

    //simulate successors for segment 2
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 31, 0.5, 1.0, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(2), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(72), updated_range.get_segment(0.2));
    assert_eq!(create_segment(62), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(31), updated_range.get_segment(0.8));

    //simulate successors for segment 31
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.66, 1.0, [31].to_vec());
    add_replacement_segment(&mut segment_map, 62, 0.33, 0.66, [31, 41].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(31), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(72), updated_range.get_segment(0.2));
    assert_eq!(create_segment(62), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(52), updated_range.get_segment(0.8));
}

/**
*       ^
        |
    1.0 +---------+---------+-------->
        |         |         |
        |    2    |         |    5
        |         |    3    |
    0.66+---------|         +-------->
        |         |         |
        |    1    |         |    6
        |         |         |
    0.33+---------+---------+-------->
        |
        |    0
        |
        +-------------------+-------->
                  1         2
*/
#[test]
fn test_replace_range_uneven_out_of_order_even_split() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.33);
    add_segment_entry(&mut segment_map, 1, 0.33, 0.66);
    add_segment_entry(&mut segment_map, 2, 0.66, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    //simulate successors for segment 1
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 31, 0.33, 1.0, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(1), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(31), updated_range.get_segment(0.4));
    assert_eq!(create_segment(31), updated_range.get_segment(0.6));
    assert_eq!(create_segment(2), updated_range.get_segment(0.8));

    //simulate successors for segment 3`
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.66, 1.0, [31].to_vec());
    add_replacement_segment(&mut segment_map, 62, 0.33, 0.66, [31].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(31), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(62), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(2), updated_range.get_segment(0.8));

    //simulate successors for segment 2
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 31, 0.33, 1.0, [1, 2].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(2), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(62), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(31), updated_range.get_segment(0.8));

    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 52, 0.66, 1.0, [31].to_vec());
    add_replacement_segment(&mut segment_map, 62, 0.33, 0.66, [31].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(31), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(0), updated_range.get_segment(0.2));
    assert_eq!(create_segment(62), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(52), updated_range.get_segment(0.8));
}

/**
* Out of order re-split
        ^
        |
    1.0 +---------+---------+-------+-------->
        |         |    5    |       |
        |         |         |       |   9
        |    1    +---------+       |
    0.66|         |    4    |       +-------->
        |         |         |       |
        |---------+---------+   6   |   8
        |         |    3    |       |
    0.33|         |         |       +-------->
        |    0    +---------+       |
        |         |    2    |       |   7
        |         |         |       |
        +---------+---------+-------+-------->
                  1         2       3
   When end of segments are encountered on 0, 2, and 6 followed by 1, 4, and 6.
*/
#[test]
fn test_replace_range_uneven_out_of_order_split() {
    let mut segment_map: BTreeMap<OrderedFloat<f64>, SegmentWithRange> = BTreeMap::new();
    add_segment_entry(&mut segment_map, 0, 0.0, 0.5);
    add_segment_entry(&mut segment_map, 1, 0.5, 1.0);
    let orig_segment_map = StreamSegments::new(segment_map);

    //simulate successors for segment 0
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 31, 0.25, 0.5, [0].to_vec());
    add_replacement_segment(&mut segment_map, 21, 0.0, 0.25, [0].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = orig_segment_map
        .apply_replacement_range(&Segment::new(0), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(21), updated_range.get_segment(0.2));
    assert_eq!(create_segment(31), updated_range.get_segment(0.4));
    assert_eq!(create_segment(1), updated_range.get_segment(0.6));
    assert_eq!(create_segment(1), updated_range.get_segment(0.8));

    //simulate successors for segment 21
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 62, 0.0, 1.0, [21, 31, 41, 51].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(21), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(62), updated_range.get_segment(0.2));
    assert_eq!(create_segment(31), updated_range.get_segment(0.4));
    assert_eq!(create_segment(1), updated_range.get_segment(0.6));
    assert_eq!(create_segment(1), updated_range.get_segment(0.8));

    //simulate successors for segment 62
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 73, 0.0, 0.33, [62].to_vec());
    add_replacement_segment(&mut segment_map, 83, 0.33, 0.66, [62].to_vec());
    add_replacement_segment(&mut segment_map, 93, 0.66, 1.0, [62].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(62), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(73), updated_range.get_segment(0.2));
    assert_eq!(create_segment(31), updated_range.get_segment(0.4));
    assert_eq!(create_segment(1), updated_range.get_segment(0.6));
    assert_eq!(create_segment(1), updated_range.get_segment(0.8));

    //simulate successors for segment 1
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 41, 0.5, 0.75, [1].to_vec());
    add_replacement_segment(&mut segment_map, 51, 0.75, 1.0, [1].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(1), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(73), updated_range.get_segment(0.2));
    assert_eq!(create_segment(31), updated_range.get_segment(0.4));
    assert_eq!(create_segment(41), updated_range.get_segment(0.6));
    assert_eq!(create_segment(51), updated_range.get_segment(0.8));

    //simulate successors for segment 41
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 62, 0.0, 1.0, [21, 31, 41, 51].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(41), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(73), updated_range.get_segment(0.2));
    assert_eq!(create_segment(31), updated_range.get_segment(0.4));
    assert_eq!(create_segment(62), updated_range.get_segment(0.6));
    assert_eq!(create_segment(51), updated_range.get_segment(0.8));

    //simulate successors for segment 62
    let mut segment_map: HashMap<SegmentWithRange, Vec<Segment>> = HashMap::new();
    add_replacement_segment(&mut segment_map, 73, 0.0, 0.33, [62].to_vec());
    add_replacement_segment(&mut segment_map, 83, 0.33, 0.66, [62].to_vec());
    add_replacement_segment(&mut segment_map, 93, 0.66, 1.0, [62].to_vec());
    let successor_for_segment = StreamSegmentsWithPredecessors::new(segment_map.into());

    let updated_range = updated_range
        .apply_replacement_range(&Segment::new(62), &successor_for_segment)
        .unwrap();

    assert_eq!(create_segment(73), updated_range.get_segment(0.2));
    assert_eq!(create_segment(31), updated_range.get_segment(0.4));
    assert_eq!(create_segment(83), updated_range.get_segment(0.6));
    assert_eq!(create_segment(51), updated_range.get_segment(0.8));
}

fn add_segment_entry(
    segment_map: &mut BTreeMap<OrderedFloat<f64>, SegmentWithRange>,
    segment: i64,
    low_key: f64,
    high_key: f64,
) {
    segment_map.insert(
        OrderedFloat(high_key),
        SegmentWithRange::new(
            create_segment(segment),
            OrderedFloat(low_key),
            OrderedFloat(high_key),
        ),
    );
}
fn add_replacement_segment(
    segment_map: &mut HashMap<SegmentWithRange, Vec<Segment>>,
    segment: i64,
    low_key: f64,
    high_key: f64,
    predecessor: Vec<i64>,
) {
    let s = predecessor.iter().map(|s| Segment::new(*s)).collect();
    segment_map.insert(
        SegmentWithRange::new(
            create_segment(segment),
            OrderedFloat(low_key),
            OrderedFloat(high_key),
        ),
        s,
    );
}

fn create_segment(segment_number: i64) -> ScopedSegment {
    ScopedSegment {
        scope: Scope::new("Scope".to_string()),
        stream: Stream::new("Stream".to_string()),
        segment: Segment::new(segment_number),
    }
}
