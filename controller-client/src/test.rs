use tokio::runtime::Runtime;

// Note this useful idiom: importing names from outer (for mod tests) scope.
use super::*;

#[test]
#[should_panic] // since the controller is not running.
fn test_create_scope_error() {
    let mut rt = Runtime::new().unwrap();

    let client_future = create_connection("http://[::1]:9090");
    let mut client = rt.block_on(client_future);

    let request = Scope {
        name: "testScope124".into(),
    };
    let fut = create_scope(request, &mut client);

    rt.block_on(fut).unwrap();
}

#[test]
#[should_panic] // since the controller is not running.
fn test_create_stream_error() {
    let mut rt = Runtime::new().unwrap();

    let client_future = create_connection("http://[::1]:9090");
    let mut client = rt.block_on(client_future);

    let request = StreamConfiguration {
        scoped_stream: ScopedStream {
            scope: Scope {
                name: "testScope123".into(),
            },
            stream: Stream {
                name: "testStream".into(),
            },
        },
        scaling: Scaling {
            scale_type: ScaleType::FixedNumSegments,
            target_rate: 0,
            scale_factor: 0,
            min_num_segments: 1,
        },
        retention: Retention {
            retention_type: RetentionType::None,
            retention_param: 0,
        },
    };
    let fut = create_stream(request, &mut client);

    rt.block_on(fut).unwrap();
}
