// Note this useful idiom: importing names from outer (for mod tests) scope.
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
