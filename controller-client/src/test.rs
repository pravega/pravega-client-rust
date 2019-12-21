
// Note this useful idiom: importing names from outer (for mod tests) scope.
use super::*;
use tokio::runtime::Runtime;

#[test]
#[should_panic] // since the controller is not running.
fn test_create_scope_error() {
    let mut rt = Runtime::new().unwrap();

    let client_future = create_connection("http://[::1]:9090");
    let mut client = rt.block_on(client_future);

    let request = ScopeInfo {
        scope: "testScope124".into(),
    };
    let fut = create_scope(request, &mut client);

    rt.block_on(fut);
}

#[test]
#[should_panic] // since the controller is not running.
fn test_create_stream_error() {
    let mut rt = Runtime::new().unwrap();

    let client_future = create_connection("http://[::1]:9090");
    let mut client = rt.block_on(client_future);

    let request = StreamConfig {
        stream_info: Some(StreamInfo {
            scope: "testScope123".into(),
            stream: "testStream".into(),
        }),
        scaling_policy: Some(ScalingPolicy {
            scale_type: ScalingPolicyType::FixedNumSegments as i32,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 1,
        }),
        retention_policy: None,
    };
    let fut = create_stream(request, &mut client);

    rt.block_on(fut);
}
