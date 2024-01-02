module main

go 1.17

require (
	github.com/pravega/pravega-client-rust/golang v0.0.1
	github.com/sirupsen/logrus v1.8.1
	golang.org/x/net v0.17.0
	golang.org/x/sync v0.1.0
)

require golang.org/x/sys v0.13.0 // indirect

replace github.com/pravega/pravega-client-rust/golang v0.0.1 => ../
