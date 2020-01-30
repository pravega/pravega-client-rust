use pravega_controller_client::*;
use pravega_rust_client_shared::*;
use std::convert::TryFrom;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error + 'static>> {
    // start Pravega standalone before invoking this function.
    let mut client = create_connection("http://[::1]:9090").await;
    let request = ScopeInfo {
        scope: "testScope123".into(),
    };
    let scope_result = create_scope(request, &mut client).await;
    println!("Response for create_scope is {:?}", scope_result);

    let request2 = StreamConfig {
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
    let stream_result: Result<bool> = create_stream(request2, &mut client).await;
    println!("Response for create_stream is {:?}", stream_result);

    let request3 = ScopedSegment {
        scope: Scope {
            name: "testScope123".into(),
        },
        stream: Stream {
            name: "testStream".into(),
        },
        segment: Segment { number: 0 },
    };
    //let get_uri_result = get_endpoint_for_segment_top(request3, &mut client).await;
    //println!("Response for get_uri is {:?}", get_uri_result);

    let mut controller_client = ControllerClientImpl {
        channel: client
    };
    let result_final = controller_client.get_endpoint_for_segment(request3).await;
    println!("Final result is {:?}", result_final);

    Ok(())
}
