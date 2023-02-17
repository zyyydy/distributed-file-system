#include <regex>
#include <mutex>
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
#include <utime.h>
#include <google/protobuf/util/time_util.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;


extern dfs_log_level_e DFS_LOG_LEVEL;
using dfs_service::File;
using dfs_service::FileStatus;
using dfs_service::Files;
using dfs_service::FileBytes;
using dfs_service::FileReply;
using dfs_service::Empty;
using dfs_service::WriteLock;

using google::protobuf::RepeatedPtrField;
using google::protobuf::util::TimeUtil;
using google::protobuf::Timestamp;

using std::chrono::system_clock;
using std::chrono::milliseconds;
using namespace std;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = dfs_service::File;
using FileListResponseType = dfs_service::Files;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    context.AddMetadata("clientid", ClientId());

    File file_request;
    file_request.set_name(filename);
    WriteLock reply;

    Status status = service_stub->RequestWriteLock(&context, file_request, &reply);
    if (!status.ok()) {
        dfs_log(LL_ERROR) << "Fail to request lock";
        return status.error_code();
    }
    dfs_log(LL_SYSINFO) << "Request lock successfully";
    return StatusCode::OK;

}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    struct stat result;
    const string& filepath = WrapPath(filename);
    if (stat(filepath.c_str(), &result) != 0){
        dfs_log(LL_ERROR) << filepath << "does not exist";
        return StatusCode::NOT_FOUND;
    }    

    ClientContext context;
    context.AddMetadata("filename", filename);
    context.AddMetadata("clientid", ClientId());
    context.AddMetadata("check_sum", to_string(dfs_file_checksum(filepath, &crc_table)));
    context.AddMetadata("mtime", to_string(static_cast<long>(result.st_mtime)));    
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));
    
    //if a lock can not be obtained
    StatusCode status_writelock = this->RequestWriteAccess(filename);
    if (status_writelock != StatusCode::OK) {
        return StatusCode::RESOURCE_EXHAUSTED;
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


grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //
    const string& filepath = WrapPath(filename);

    ClientContext context;
    context.AddMetadata("check_sum", to_string(dfs_file_checksum(filepath, &crc_table)));
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));

    struct stat result;
    
    // add mtime if file exists
    if (stat(filepath.c_str(), &result) == 0){
        dfs_log(LL_SYSINFO) << filepath << ": add mtime to metadata";
        context.AddMetadata("mtime", to_string(static_cast<long>(result.st_mtime)));
    }    

    File file_fetch;
    file_fetch.set_name(filename);
    
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
        dfs_log(LL_ERROR) << "Error in get the file: " << err.what();
        rep.release();
        return StatusCode::CANCELLED;
    }
    Status status = rep->Finish();
    if(!status.ok()){
        dfs_log(LL_ERROR) << "Fetch response message: " << status.error_message();
        if (status.error_code() == StatusCode::INTERNAL){
            return StatusCode::CANCELLED;
        }
    }
    return status.error_code();

}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    StatusCode status_writelock = this->RequestWriteAccess(filename);
    if (status_writelock != StatusCode::OK) {
        return StatusCode::RESOURCE_EXHAUSTED;
    }    

    ClientContext context;
    context.AddMetadata("clientid", ClientId());
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));

    File file_delete;
    file_delete.set_name(filename);
    FileReply reply;

    Status status = service_stub->DeleteFile(&context, file_delete, &reply);

    if (!status.ok()){
        dfs_log(LL_ERROR) << "fail to delete " << status.error_message();
        if (status.error_code() == StatusCode::INTERNAL){
            return StatusCode::CANCELLED;
        }
    }
    dfs_log(LL_SYSINFO) << "Deleted file sucessfully" << reply.DebugString();
    return status.error_code();    

}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
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
        dfs_log(LL_ERROR) << "Failed to List All files" << status.error_message();
        if (status.error_code() == StatusCode::INTERNAL){
            return StatusCode::CANCELLED;
        }
    }
    dfs_log(LL_SYSINFO) << "List All Files Successfully";

    for (const FileStatus& rep: reply.file()){
        int time_modified = TimeUtil::TimestampToSeconds(rep.time_modified());
        file_map->insert(pair<string, int>(rep.name(), time_modified));
        dfs_log(LL_SYSINFO) << rep.name() << " is added to file map";
    }
    return status.error_code();

}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
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
    file_request.set_name(filename);
    FileStatus statusreply;

    Status status = service_stub->GetFileStatus(&context, file_request, &statusreply);
    if (!status.ok()) {
        dfs_log(LL_ERROR) << "Failed to get the status: " << status.error_message();
        if (status.error_code() == StatusCode::INTERNAL) {
            return StatusCode::CANCELLED;
        }
    }
    dfs_log(LL_SYSINFO) << "Status is obtained Successfully - response: " << statusreply.DebugString();
   
    file_status = &statusreply;
    return status.error_code();

}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //
    dir_mutex.lock();
    dfs_log(LL_SYSINFO) << "InotifyWatcherCallback";
    callback();
    dir_mutex.unlock();
}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }
            
            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //
                dir_mutex.lock();

                for (const FileStatus& status_file_server : call_data->reply.file()) {
                    const string& filepath = WrapPath(status_file_server.name());

                    StatusCode status_code;
                    struct stat status_file_client;
                    
                    // Fetch the file from the server if it doesnt exist on the client side 
                    if (stat(filepath.c_str(), &status_file_client) != 0) {
                        dfs_log(LL_SYSINFO) << "File " << status_file_server.name() << " doesn't exist";
                        status_code = this->Fetch(status_file_server.name());
                        if (status_code != StatusCode::OK) {
                            dfs_log(LL_ERROR) << "Fail to Fetch file ";
                        }
                    
                    // Fetch it if local timestamp < remote
                    } else if (status_file_server.time_modified() > Timestamp(TimeUtil::TimeTToTimestamp(status_file_client.st_mtime))) {

                        dfs_log(LL_SYSINFO) << "Fetch" << status_file_server.name() << " as the local one is out of date";
                        status_code = this->Fetch(status_file_server.name());
                        if (status_code == StatusCode::ALREADY_EXISTS) {
            
                            struct utimbuf new_mtime;
                            new_mtime.modtime = TimeUtil::TimestampToTimeT(status_file_server.time_modified());
                            if (!utime(filepath.c_str(), &new_mtime)) {
                                dfs_log(LL_SYSINFO) << "Updated mtime for " << filepath;
                            } else {
                                dfs_log(LL_ERROR) << "Failed to update mtime for " << filepath;
                            }

                        } else if (status_code != StatusCode::OK) {
                            dfs_log(LL_ERROR) << "Fail to Fetch file ";
                        }

                    // Store the file if the local one is more updated 
                    } else if (Timestamp(TimeUtil::TimeTToTimestamp(status_file_client.st_mtime)) > status_file_server.time_modified()) {
                        dfs_log(LL_SYSINFO) << "File " << status_file_server.name() << " is out of date on server. " ;
                        status_code = this->Store(status_file_server.name());
                        if (status_code != StatusCode::OK) {
                            dfs_log(LL_ERROR) << "Fail to Store file ";
                        }
                    }
                }

                dir_mutex.unlock();

            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//


