fn main() {
    tonic_build::compile_protos("./proto/Controller.proto").unwrap();
}
