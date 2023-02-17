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

Here are the high-level sequence digagram [docs/part1-sequence.pdf](docs/part1-sequence.pdf) and class diagram [docs/part1-class-diagram.jpg](docs/part1-class-diagram.jpg)


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

## Part 2: Completing the Distributed File System (DFS)
The goal of part 2 is to complete a rudimentary DFS. It applies a weakly consistent cache strategy to the RPC calls that are created in part1. In addition, it also make an asynchronous gRPC call to allow the server to communicate to connected clients whenever a change is registered in the service.

Below are expectation of part 2: 

* **Whole-file caching**. The client should cache whole files on the client-side. Read and write operations should be applied to local files and only update the server when a file has been modified or created on the client.

* **One Creator/Writer per File (i.e., writer locks)**. Clients should request a file write lock from the server before pushing. If they are not able to obtain a lock, the attempt should fail. The server should keep track of which client holds a lock to a particular file, then release that lock after the client has successfully stored a file to the server.

* **Client initiated Deletions**. Deletion requests should come from a client, but be broadcast to other clients attached to the server. In other words, if Client1 deletes a file locally, it should send a deletion request to the server as well. The server should then delete it from its own cache and then notify all clients that the file has been deleted.

* **Synchronized Clients and Server**. The folders for the server and all clients stay in sync. In other words, the server's storage and all of the clients' mount storage directories should be equal.

* **CRC Checksums**. To determine if a file has changed between the server and the client, a CRC checksum function is used. Any differences in the checksums between the server and the client constitute a change. 

* **Date based Sequences**. If a file has changed between the server and the client, then the last modified timestamp should win. In other words, if the server has a more recent timestamp, then the client should fetch it from the server. If the client has a more recent timestamp, then the client should store it on the server. If there is no change in the modified time for a file, the client should do nothing. Keep in mind, when storing data, you must first request a write lock as described earlier.

Here are the high-level sequence digagram [docs/part2-sequence.pdf](docs/part2-sequence.pdf) and class diagram [docs/part2-class-diagram.jpg](docs/part2-class-diagram.jpg)

To run the code:
```
make protos
make
```
Test 1 
```
./bin/dfs-server-p2
./bin/dfs-client-p2 mount
```
Test 2
```
./bin/dfs-server-p2
./bin/dfs-client-p2 fetch gt-klaus.jpg
```
Test 3
```
./bin/dfs-server-p2
./bin/dfs-client-p2 delete gt-klaus.jpg
```
Test 4
```
./bin/dfs-server-p2
./bin/dfs-client-p2 store gt-klaus.jpg
```

