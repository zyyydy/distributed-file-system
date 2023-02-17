# Part 1: Building the RPC protocol service

In Part 1, you will build a series of remote procedure calls (RPC) and message types that will fetch, store, list, and get attributes for files on a remote server. The SunRPC implementation (ONC RPC) and XDR interface definition language (IDL) you learned about in the lectures are currently deprecated in favor of more modern implementations of the concepts behind RPC and IDL.  There are many RPC replacements, including [TI-RPC](https://docs.oracle.com/cd/E19683-01/816-1435/rpcintro-46812/), [Finagle](https://twitter.github.io/finagle/), [Thrift](https://thrift.apache.org), and [Cap'n Proto](https://capnproto.org/). However, in this assignment, we will use [gRPC](https://grpc.io/) for RPC services and [Protocol Buffers](https://developers.google.com/protocol-buffers/) as the definition language. The core gRPC library is written in C but supports multiple languages, including C++, Java, Go, and others. It is actively developed and in use at several organizations, such as Google, Square, Netflix, Juniper, Cisco, and Dropbox. In this assignment, we will use the gRPC C++ API.

## Part 1 Goals

The goal of part 1 is to generate an RPC service that will perform the following operations on a file:

* Fetch a file from a remote server and transfer its contents via gRPC
* Store a file to a remote server and transfer its data via gRPC
* List all files on the remote server:

    * For this assignment, the server is only required to contain files in a single directory; it is not necessary to manage nested directories.
    * The file listing should include the file name and the modified time (_mtime_) of the file data in seconds from the epoch.

* Get the following attributes for a file on the remote server:

    *  Size
    *  Modified Time
    * Creation Time

The client should be able to request each of the operations described above for binary and text-based files. The server will respond to those requests using the gRPC service methods you specify in your proto buffer definition file.

* Finally, the client should recognize when the server has timed out. gRPC can signal the server using a deadline timeout as described in [this gRPC article](https://grpc.io/blog/deadlines/). You should ensure that your client recognizes this timeout signal.

### Part 1 Sequence and Class Diagrams

A high-level sequence diagram of the expected interactions in part 1 is available in the [docs/part1-sequence.pdf](part1-sequence.pdf) file of this repository.

A high-level class diagram is avaliable in [docs/part1-class-diagram.jpg](part1-class-diagram.jpg). However, you are only required to understand and work with the student code sections indicated.

## Protocol Buffers and gRPC

To begin part 1, you should first familiarize yourself with the basics of using Protocol Buffers. In particular, you should focus on the use of RPC service definitions and message type definitions for the request and response types that are used by the RPC services.

You will then create your protocol in the [dfs-service.proto](dfs-service.proto) file inside the project repository. There are several required services and message types described in the proto file that you should implement, but you may add as many additional methods and/or message types that you deem necessary. What you name those services and message types is also up to your discretion.

To autogenerate the gRPC and Protocol Buffer class and header files, we’ve provided a Makefile command that will take care of that for you. When you are ready to generate your protobuf/gRPC files, run the following command from the root of the repository:

```
make protos
```

You will find the results of that command in the [part1/proto-src](part1/proto-src) directory of the repository. You should familiarize yourself with the results in that directory, but you won’t need to, and should not, make any changes to those files. Your job will be to override the service methods in your [dfslib-servernode-p1.cpp](dfslib-servnode-p1.cpp) source file.

Once you have familiarized yourself with Protocol Buffers, you should next familiarize yourself with the [C++ API for gRPC](https://grpc.github.io/grpc/cpp/index.html). In particular, pay close attention to how the server implementation overrides methods, and the client makes calls to the RPC service for streaming message types.

> You do not need to concern yourself with asynchronous gRPC, we will only be working with the synchronous version in this project.

## Part 1 Structure

All of the part 1 files are available in the [part1](part1) directory. You will find several source files in that directory, but you are only responsible for adjusting and submitting the `dfslib-*` files inside `part1`.  The rest of the source files provide the supporting structure for the program. You may change any of the other source files for your testing purposes, but they will not be submitted as a part of your grade.

In each of the files to be modified, you will find additional instructions and hints on how you should approach the contents of that file. The following comment marker precedes each tip in the source code:

```
// STUDENT INSTRUCTION:
```

#### Source code file descriptions:

* `src/dfs-client-p1.[cpp,h]` - the CLI executable for the client side.

* `src/dfs-server-p1.[cpp]` - the CLI executable for the server side.

* `src/dfslibx-clientnode.[cpp,h]` - the parent class for the client node library file that you will override. All of the methods you will override are documented in the `dfslib-clientnode-p1.h` file you will modify.

* `src/dfs-utils.h` - A header file of utilities used by the executables. You may change this, but note that this file is not submitted. There is a separate `dfs-shared` file you may use for your utilities.

* `dfs-service.proto` - **TO BE MODIFIED BY STUDENT** Add your proto buffer service and message types to this file, then run the `make protos` command to generate the source.

* `dfslib-servernode-p1.[cpp,h]` - **TO BE MODIFIED BY STUDENT** - Override your gRPC service methods in this file by adding them to the `DFSServerImpl` class. The service method signatures can be found in the `proto-src/dfs-service.grpc.pb.h` file generated by the `make protos` command you ran earlier.

* `dfslib-clientnode-p1.[cpp,h]` - **TO BE MODIFIED BY STUDENT** -  Add your client-side calls to the gRPC service in this file. We’ve provided the basic structure and method calls expected by the client executable. However, you may add any additional declarations and definitions that you deem necessary.

* `dfslib-shared-p1.[cpp,h]` - **TO BE MODIFIED BY STUDENT** - Add any shared code or utilities that you need in this file. The shared header is available on both the client and server side.

## Part 1 Compiling and Running

To compile the source code in Part 1, you may use the Makefile in the root of the repository and run:

```
make part1
```

Or, you may change to the `part1` directory and run `make`.

> For a list of all make commands available, run `make` in the root of the repository.

To run the executables, see the usage instructions in their respective files.

In most cases, you'll start the server with:

```
./bin/dfs-server-p1
```

The client is then used to fetch, store, list, and stat files. For example:

```
./bin/dfs-client-p1 fetch gt-campanile.jpg
```

## Submission Instructions

Beginning this semester, we have moved to a new auto-grading system based upon Gradescope.  You can find Gradescope from Canvas, and that will make the submission engine available to you.  You will need to manually upload your code to Gradescope, where it will be executed in its own environment (a Docker container) and the results will be gathered up and returned to you.

Note that there is a strict limit to the amount of feedback that we can return to you.  Thus:

* We do not return feedback for tests that pass
* We truncate any feedback past the limit (approximately 10KB)
* We do not return detailed feedback for tests that run after the 10KB limit is reached.

As of this writing, we are actively completing our work for the new auto-grader and are staging its availability over the next several days, so you may begin working on the projects now and submitting them as we open subsequent portions of the auto-grader.

For Parts 1 & 2 you will have a limit of no more than 50 submissions between now and the final due date; you may use them all within one day, or you may use one per day - the choice is yours.

We strongly encourage you to think about testing your own code.  Test driven development is a standard technique for software, including systems software.  The auto-grader is a _grader_ and not a test suite.

Note: your project report (10% of your grade for Project 4) is submitted on **Canvas** in either markdown or PDF format.

**Note**: There is no limit to the number of times you may submit your **readme-student.md** or **readme-student.pdf** file.

The Project 4 grading is a little different from the other projects.  So, you **may** experience much longer wait times with Project 4.

Once again, we strongly encourage you to think about testing your own code.  Test driven development is a standard technique for software, including systems software.  The autograder is _not_ your test suite.
