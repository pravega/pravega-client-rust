use pravega_controller_client::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
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
    let stream_result: Result<bool, ControllerError> = create_stream(request2, &mut client).await;
    println!("Response for create_stream is {:?}", stream_result);

    Ok(())
}
