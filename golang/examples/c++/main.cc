#include <iostream>
extern "C"
{
    #include <pravega_client.h>
}

int main() {
    StreamManager* manager = stream_manager_new("127.0.0.1:9090");
    std::cout << "create scope test: " << stream_manager_create_scope(manager, "test") << std::endl;
    stream_manager_destroy(manager);
    return 0;
}
