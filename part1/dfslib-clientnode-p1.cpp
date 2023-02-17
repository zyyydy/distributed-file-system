#include <regex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <google/protobuf/util/time_util.h>

#include "dfslib-shared-p1.h"
#include "dfslib-clientnode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//

using dfs_service::File;
using dfs_service::FileStatus;
using dfs_service::Files;
using dfs_service::FileBytes;
using dfs_service::FileReply;
using dfs_service::Empty;

using google::protobuf::RepeatedPtrField;
using google::protobuf::util::TimeUtil;

using std::chrono::system_clock;
using std::chrono::milliseconds;
using namespace std;

DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    context.AddMetadata("filename", filename);
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));

    struct stat result;
    const string& filepath = WrapPath(filename);
    if (stat(filepath.c_str(), &result) != 0){
        dfs_log(LL_ERROR) << filepath << "does not exist";
        return StatusCode::NOT_FOUND;
    }

    int filelen = result.st_size;
    FileReply reply;
    unique_ptr<ClientWriter<FileBytes>> rep = service_stub->StoreFile(&context, &reply);
    dfs_log(LL_SYSINFO) << "Store to store " << filepath << "with size of " << filelen;
    ifstream ifs(filepath);

    FileBytes file_bytes;
    int bytes_total= 0;
    try{
        while(!ifs.eof() && bytes_total < filelen){

            int bytes_transfered = min(filelen - bytes_total, chunk_len);
            char buffer[bytes_transfered];

            ifs.read(buffer, bytes_transfered);
            file_bytes.set_bytes_chunk(buffer, bytes_transfered);
            rep->Write(file_bytes);
            bytes_total += bytes_transfered;

            dfs_log(LL_SYSINFO) << "Stored " << bytes_transfered << " of " << filelen << " bytes";
        }

        ifs.close();
        if (bytes_total != filelen){
            return StatusCode::CANCELLED;
        }

    } catch (exception const& err){

        dfs_log(LL_ERROR) << "ERROR in storing file: " << err.what();
        rep.release();
        return StatusCode::CANCELLED;

    }
    rep->WritesDone();
    Status status = rep->Finish();
    if (!status.ok()){
        dfs_log(LL_ERROR) << "Store response message: " << status.error_message() << " code:" << status.error_code();
        if (status.error_code() == StatusCode::INTERNAL){
            return StatusCode::CANCELLED;
        }
    }
    dfs_log(LL_SYSINFO) << "Successfully finished storing: " << reply.DebugString();
    return status.error_code();
}


StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));

    File file_fetch;
    file_fetch.set_filename(filename);
    
    const string& filepath = WrapPath(filename);
    unique_ptr<ClientReader<FileBytes>> rep = service_stub->FetchFile(&context, file_fetch);
    ofstream ofs;
    FileBytes file_bytes;
    try{
        while(rep->Read(&file_bytes)){
            if(!ofs.is_open()){
                ofs.open(filepath, ios::trunc);
            }
            const string& content = file_bytes.bytes_chunk();
            ofs << content;
        }
        ofs.close();
    } catch (exception const& err){
        dfs_log(LL_ERROR) << "Error writing to file: " << err.what();
        rep.release();
        return StatusCode::CANCELLED;
    }
    Status status = rep->Finish();
    if(!status.ok()){
        dfs_log(LL_ERROR) << "Fetch response message: " << status.error_message() << " code: " << status.error_code();
        if (status.error_code() == StatusCode::INTERNAL){
            return StatusCode::CANCELLED;
        }
    }
    return status.error_code();
}

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));

    File file_delete;
    FileReply reply;

    file_delete.set_filename(filename);
    Status status = service_stub->DeleteFile(&context, file_delete, &reply);

    if (!status.ok()){
        dfs_log(LL_ERROR) << "fail to delete " << status.error_message() << ", code: " << status.error_code();
        if (status.error_code() == StatusCode::INTERNAL){
            return StatusCode::CANCELLED;
        }
    }
    dfs_log(LL_SYSINFO) << "Deleted file sucessfully" << reply.DebugString();
    return status.error_code();
}

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));

    Empty request;
    Files reply;

    Status status = service_stub->ListAllFiles(&context, request, &reply);
    if (!status.ok()){
        dfs_log(LL_ERROR) << "List All failed - message: " << status.error_message() << ", code: " << status.error_code();
        if (status.error_code() == StatusCode::INTERNAL){
            return StatusCode::CANCELLED;
        }
    }
    dfs_log(LL_SYSINFO) << "List All Files:" << reply.DebugString();

    for (const FileReply& rep: reply.file()){
        int time_modified = TimeUtil::TimestampToSeconds(rep.time_modified());
        file_map->insert(pair<string, int>(rep.filename(), time_modified));
        dfs_log(LL_SYSINFO) << rep.filename() << " is added to file map";
    }
    return status.error_code();
}

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));

    File file_request;
    file_request.set_filename(filename);
    FileStatus statusreply;

    Status status = service_stub->GetFileStatus(&context, file_request, &statusreply);
    if (!status.ok()) {
        dfs_log(LL_ERROR) << "List files failed - message: " << status.error_message() << ", code: " << status.error_code();
        if (status.error_code() == StatusCode::INTERNAL) {
            return StatusCode::CANCELLED;
        }
    }
    dfs_log(LL_SYSINFO) << "Success - response: " << statusreply.DebugString();
   
    file_status = &statusreply;
    return status.error_code();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//

