 GRPC and Distributed Systems

This project will design and implement a simple distributed file system (DFS).  First, several file transfer protocols are developed using gRPC and Protocol Buffers. Next, it incorporates a weakly consistent synchronization system to manage cache consistency between multiple clients and a single server. The system is able to handle both binary and text-based files.

## Part 1: Building the RPC protocol service

The goal of part 1 is to generate an RPC service that will perform the following operations on a file:

* Fetch a file from a remote server and transfer its contents via gRPC
* Store a file to a remote server and transfer its data via gRPC
* List all files including filename and modified time on the remote server.
* Get the following attributes for a file on the remote server:

    *  Size
    *  Modified Time
    *  Creation Time

The client should be able to request each of the operations described above for binary and text-based files. The server will respond to those requests using the gRPC service methods you specify in your proto buffer definition file. Finally, the client should recognize when the server has timed out. 

Here are the high-level sequence digagram[docs/part1-sequence.pdf](docs/part1-sequence.pdf) and class diagram[docs/part1-class-diagram.jpg](docs/part1-class-diagram.jpg)


To run the code:
```
make protos
make
```
Test 1:
```
./bin/dfs-server-p1
./bin/dfs-client-p1 fetch gt-klaus.jpg
```
Test 2
```
./bin/dfs-server-p1
./bin/dfs-client-p1 delete gt-klaus.jpg
```
Test 3
```
./bin/dfs-server-p1
./bin/dfs-client-p1 store gt-klaus.jpg
```

Part 2: Completing the Distributed File System (DFS)
In this part, cache process is added to the RPC calls and it will make asynchronous gRPC call. 

To run the code:
make protos

make

Test 1 
./bin/dfs-server-p2
./bin/dfs-client-p2 mount

Test 2
./bin/dfs-server-p2
./bin/dfs-client-p2 fetch gt-klaus.jpg

Test 3
./bin/dfs-server-p2
./bin/dfs-client-p2 delete gt-klaus.jpg

Test 4
./bin/dfs-server-p2
./bin/dfs-client-p2 store gt-klaus.jpg


