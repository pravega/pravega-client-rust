
# Praveglet

Praveglet is an app that can be used to interact with Pravega. This utility can perform the following operations
* Create a Scope
* Create a Stream
* Write events into a Stream from STDIN or FIFO.
* List Scopes
* List Streams
* List Streams for the provided Tag.

### Usage documentation:
```
Praveglet 0.0.1
Utility to interact with Pravega

USAGE:
praveglet [FLAGS] [OPTIONS] <SUBCOMMAND>

FLAGS:
-a, --enable-auth    Enable authorization, default is false
-h, --help           Prints help information
-V, --version        Prints version information

OPTIONS:
-u, --controller-uri <controller-uri>  To enable TLS use uri of the format
tls://ip:port [default: tcp://127.0.0.1:9090]

SUBCOMMANDS:
create-scope            Create Scope
create-stream           Create Stream with a fixed segment count
help                    Prints this message or the help of the given subcommand(s)
list-scopes             List Scopes
list-streams            List Streams under a scope
list-streams-for-tag    List Streams for a tag, under a scope
write                   Write events from a file
```

### Building
To build Praveglet
```
$ git clone https://github.com/pravega/pravega-client-rust.git
$ cd pravega-client-rust/praveglet
$ cargo build --release
$ ../target/release/praveglet -V
$ Praveglet 0.0.1
```