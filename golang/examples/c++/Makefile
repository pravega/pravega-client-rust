all: main.cc ../../pkg/pravega_client.h
	g++ main.cc -I../../pkg/ -L../../../target/debug -lpravega_client_c -Wl,-rpath,../../../target/debug -o main

clean:
	rm main
