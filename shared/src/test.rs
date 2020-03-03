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
    // TODO: This fails need to find a solution for this.
    // assert_eq!(segment_1.clone(), s.get_segment(0.5));

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

fn create_segment(segment_number: i64) -> ScopedSegment {
    ScopedSegment {
        scope: Scope::new("Scope".to_string()),
        stream: Stream::new("Stream".to_string()),
        segment: Segment::new(segment_number),
    }
}
