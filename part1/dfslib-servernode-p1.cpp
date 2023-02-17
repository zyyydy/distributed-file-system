#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include <google/protobuf/util/time_util.h>

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;
using grpc::string_ref;

using namespace std;

using dfs_service::DFSService;
using dfs_service::FileStatus;
using dfs_service::FileBytes;
using dfs_service::FileReply;
using dfs_service::File;
using dfs_service::Files;
using dfs_service::Empty;

using google::protobuf::uint64;
using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;

//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //
    // Method to store a file 
    Status StoreFile(
        ServerContext* context,
        ServerReader<FileBytes>* reader,
        FileReply* reply
    ) override {
        const multimap<string_ref, string_ref>& metadata = context->client_metadata();
        auto filename_all = metadata.find("filename");
        stringstream err_message;

        // filename not found
        if (filename_all == metadata.end()){
            err_message << "filename not found in the client request" << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        }

        auto filename = string(filename_all->second.begin(), filename_all->second.end());
        const string& filepath = WrapPath(filename);

        dfs_log(LL_SYSINFO) << "Start to store the file " << filepath << "on the server";
        FileBytes file_bytes;
        ofstream ofs;
        try {
            while (reader -> Read(&file_bytes)){

                if(!ofs.is_open()){
                    ofs.open(filepath, ios::trunc);
                }

                if (context->IsCancelled()){
                    err_message <<  "Deadline exceeded" << endl;
                    dfs_log(LL_ERROR) << err_message.str();
                    return Status(StatusCode::DEADLINE_EXCEEDED, err_message.str());
                }

                ofs << file_bytes.bytes_chunk();
            }
            ofs.close();

        } catch (exception const& err) {
            err_message << "Error in storing file " << err.what() << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        }

        struct stat result;
        if (stat(filepath.c_str(), &result) != 0) {
            err_message << "Fail to retrieve fileinfo of " << filepath << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::NOT_FOUND, err_message.str());
        }

        reply->set_filename(filename);
        Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_mtime));
        reply->set_allocated_time_modified(time_modified);
        dfs_log(LL_SYSINFO) << "Completed storing " << filepath;
        return Status::OK;
    }

    Status FetchFile(
        ServerContext* context,
        const File* file_fetch,
        ServerWriter<FileBytes>* writer
    ) override {
        const string& filepath = WrapPath(file_fetch->filename());
        struct stat result;

        stringstream err_message;

        if (stat(filepath.c_str(), &result) != 0){
            err_message << filepath << " does not exist" << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::NOT_FOUND, err_message.str());
        }

        dfs_log(LL_SYSINFO) << "Start to fetch file" << filepath;
        int filelen = result.st_size;
        ifstream ifs(filepath);
        FileBytes file_bytes;
        try{
            int bytes_total = 0;
            while(bytes_total < filelen && !ifs.eof()){
                int bytes_transfered = min(filelen - bytes_total, chunk_len);
                char buffer[bytes_transfered];
                if (context->IsCancelled()){
                    err_message << "Request deadline has expired" << endl;
                    dfs_log(LL_ERROR) << err_message.str();
                    return Status(StatusCode::DEADLINE_EXCEEDED, err_message.str());
                }
                ifs.read(buffer, bytes_transfered);
                file_bytes.set_bytes_chunk(buffer, bytes_transfered);
                writer->Write(file_bytes);
                bytes_total += bytes_transfered;
            }
            ifs.close();
            if (bytes_total != filelen){
                err_message << "The impossible happened" << endl;
                dfs_log(LL_ERROR) << err_message.str();
                return Status(StatusCode::INTERNAL, err_message.str());
            }
        } catch (exception const& err) {
            err_message << "Error reading file" <<err.what() << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        }
        dfs_log(LL_SYSINFO) << "Completed fetching " << filepath;
        return Status::OK;
    }

    Status DeleteFile(
        ServerContext* context,
        const File* file_delete,
        FileReply* reply
    ) override {
        const string& filepath = WrapPath(file_delete->filename());
        dfs_log(LL_SYSINFO) << "Start to delete " << filepath;
        stringstream err_message;
        // File doesnt exist
        struct stat result;
        if (stat(filepath.c_str(), &result) != 0){
            err_message << "File " << filepath << " doesnt exit" << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::NOT_FOUND, err_message.str());
        }
        if (context->IsCancelled()){
            dfs_log(LL_ERROR) << "Request deadline has expired";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Request deadline has expired");
        }
        // Delete file 
        if (remove(filepath.c_str()) != 0){
            err_message << "Removing file" << filepath << " failed with:" << strerror(errno) << endl;
            return Status(StatusCode::INTERNAL, err_message.str());
        }
        reply->set_filename(file_delete->filename());
        Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_mtime));
        reply->set_allocated_time_modified(time_modified);
        dfs_log(LL_SYSINFO) << "Completed deleting file successfully" << filepath;
        return Status::OK;
    }

    Status ListAllFiles(
        ServerContext* context,
        const Empty* request,
        Files* reply
    ) override {
        DIR *dir;
        struct dirent *ent;
        stringstream err_message;
        if ((dir = opendir(mount_path.c_str())) == NULL) {
            /* could not open directory */
            err_message << "Failed to open mountpath " << mount_path << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::NOT_FOUND, err_message.str());;
        }
        /* traverse everything in directory */
        while ((ent = readdir(dir)) != NULL){
            if (context->IsCancelled()){
                dfs_log(LL_ERROR) << "Request deadline has expired";
                return Status(StatusCode::DEADLINE_EXCEEDED, "Request deadline has expired");
            }
            string dirEntry(ent->d_name);
            string filepath = WrapPath(dirEntry);
            FileReply* rep = reply->add_file();
            stringstream err_message;
            struct stat result;
            if (stat(filepath.c_str(), &result) != 0){
                err_message << "Getting file info for file" << filepath << " failed with: " << endl;
                dfs_log(LL_ERROR) << err_message.str();
                return Status(StatusCode::NOT_FOUND, err_message.str());
            }
            rep->set_filename(dirEntry);
            Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_mtime));
            rep->set_allocated_time_modified(time_modified);
        }
        closedir(dir);
        return Status::OK;
    }

    Status GetFileStatus(
        ServerContext* context,
        const File* request_file,
        FileStatus* file_status
    ) override{
        
        if(context->IsCancelled()){
            dfs_log(LL_ERROR) << "Request deadline has expired";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Request deadline has expired");
        }

        string filepath = WrapPath(request_file->filename());
        struct stat result;
        stringstream err_message;
        /* Get FileStatus of file*/
        if (stat(filepath.c_str(), &result) != 0){
            err_message << "Start to get the info of " << filepath << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::NOT_FOUND, err_message.str());
        }
        
        file_status->set_filename(filepath);
        file_status->set_filelen(result.st_size);
        Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_mtime));
        Timestamp* time_created = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_ctime));
        file_status->set_allocated_time_modified(time_modified);
        file_status->set_allocated_time_created(time_created);

        return Status::OK;
    }


};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//

