#include <iostream>
extern "C"
{
    #include <pravega_client.h>
}

int main() {
    StreamManager* manager = stream_manager_new("127.0.0.1:9090", nullptr);
    std::cout << "create scope foo: " << stream_manager_create_scope(manager, "foo", nullptr) << std::endl;
    std::cout << "create stream bar: " << stream_manager_create_stream(manager, "foo", "bar", 1,  nullptr) << std::endl;
    StreamWriter* writer = stream_writer_new(manager, "foo", "bar", 10, nullptr);
    std::cout << "stream writer created\n";
    const char* event = "hello";
    const char* routing_key = "world";
    Buffer b_event {(uint8_t*)event, 5, 5};
    Buffer b_routing_key {(uint8_t*)routing_key, 5, 5};
    stream_writer_write_event(writer, b_event, b_routing_key, nullptr);
    std::cout << "event wrote\n";
    stream_writer_flush(writer, nullptr);
    std::cout << "event flushed\n";
    stream_writer_destroy(writer);
    stream_manager_destroy(manager);
    return 0;
}
