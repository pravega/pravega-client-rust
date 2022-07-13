module main

go 1.17

require (
	github.com/pravega/pravega-client-rust/golang v0.0.1
	github.com/sirupsen/logrus v1.8.1
	golang.org/x/net v0.0.0-20220624214902-1bab6f366d9e
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f
)

require golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect

replace github.com/pravega/pravega-client-rust/golang v0.0.1 => ../
