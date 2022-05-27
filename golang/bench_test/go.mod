module main

go 1.17

require (
	github.com/google/uuid v1.3.0
	github.com/sirupsen/logrus v1.8.1
	github.com/pravega/pravega-client-rust/golang v0.0.1
	golang.org/x/net v0.0.0-20220526153639-5463443f8c37
	golang.org/x/sync v0.0.0-20220513210516-0976fa681c29
)

require golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
replace github.com/pravega/pravega-client-rust/golang v0.0.1 => ../
