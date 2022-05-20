all: main.cc pravega_client.h
	g++ main.cc -I. -L../../target/debug -lpravega_client_c -o main -Wl,-rpath,../../target/debug

clean:
	rm main
