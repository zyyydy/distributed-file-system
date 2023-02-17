#include <map>
#include <mutex>
#include <shared_mutex>
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


#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "dfslib-servernode-p2.h"
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
using dfs_service::WriteLock;

using google::protobuf::uint64;
using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = dfs_service::File;
using FileListResponseType = dfs_service::Files;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** The mount path for the server **/
    std::string mount_path;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;


    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

    // Stores which client id has a write lock on which file name
    map<string, string> filename_clientid;
    shared_timed_mutex filename_clientid_mutex;
    
    shared_timed_mutex filename_write_mutex;
    map<string, unique_ptr<shared_timed_mutex>> filename_write_mutex_map;
    shared_timed_mutex dir_mutex;

    // client and server should have different checksum 
    Status verifyChecksum(const multimap<string_ref, string_ref>& metadata, string& filepath) {

        auto check_sum_all = metadata.find("check_sum");
        stringstream err_message;

        if (check_sum_all == metadata.end()){
            err_message << "Missing checksum in client metadata" << endl;
            return Status(StatusCode::INTERNAL, err_message.str());
        }

        unsigned long check_sum_client = stoul(string(check_sum_all->second.begin(), check_sum_all->second.end()));
        unsigned long check_sum_server = dfs_file_checksum(filepath, &crc_table);
        
        if (check_sum_client == check_sum_server) {
            err_message << "File " << filepath << " contents are the same on client and server" << endl;
            return Status(StatusCode::ALREADY_EXISTS, err_message.str());
        }
        return Status::OK;
    }    

public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
        
        Status status = this->CallbackList(context, request, response);

        if (!status.ok()) {
            dfs_log(LL_ERROR) << "Fail to CallbackList for " << request->name();
            return;
        }
        dfs_log(LL_DEBUG2) << "Succeed to CallbackList for " << request->name();
        return;

    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //


            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //
    Status RequestWriteLock(
        ServerContext* context,
        const File* file_lock,
        WriteLock* reply
    ) override {
        const multimap<string_ref, string_ref>& metadata = context->client_metadata();
        
        stringstream err_message;
        auto clientid_all = metadata.find("clientid");
        // client id not found
        if (clientid_all == metadata.end()){
            err_message << "filename not found in the client request" << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        }        
        auto clientid = string(clientid_all->second.begin(), clientid_all->second.end());

        filename_clientid_mutex.lock();
        // check the current filename client id map file
        auto clientid_lock_m = filename_clientid.find(file_lock->name());
        string clientid_lock;

        // Lock is used by other clients
        if (clientid_lock_m != filename_clientid.end() && (clientid_lock = clientid_lock_m->second).compare(clientid) != 0){
            filename_clientid_mutex.unlock();
            err_message << "Failed to request a clock as lock is already exist from " << clientid_lock << endl; 
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::RESOURCE_EXHAUSTED, err_message.str());
        // current client already put a lock on the file. 
        } else if (clientid_lock.compare(clientid) == 0) {

            filename_clientid_mutex.unlock();
            dfs_log(LL_SYSINFO) << "client id " << clientid << " already locks " << file_lock->name();
            return Status::OK;
        }
        // there is no lock on the file
        filename_clientid[file_lock->name()] = clientid;
        filename_clientid_mutex.unlock();

        // add a mutex to the file
        filename_write_mutex.lock();
        if (filename_write_mutex_map.end() == filename_write_mutex_map.find(file_lock->name())){
            filename_write_mutex_map[file_lock->name()] = make_unique<shared_timed_mutex>();
        }
        filename_write_mutex.unlock();

        return Status::OK;
    }

    // Method to store a file 
    Status StoreFile(
        ServerContext* context,
        ServerReader<FileBytes>* reader,
        FileReply* reply
    ) override {
        const multimap<string_ref, string_ref>& metadata = context->client_metadata();
        auto filename_all = metadata.find("filename");
        auto clientid_all = metadata.find("clientid");
        auto mtime_all = metadata.find("mtime");

        stringstream err_message;

        // filename not found
        if (filename_all == metadata.end()){
            err_message << "filename not found in the client request" << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        // client id not found
        } else if (clientid_all == metadata.end()){
            err_message << "client id not found " << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        } else if (mtime_all == metadata.end()){
            err_message << "modified time not found " << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());            
        }
        
        long mtime = stol(string(mtime_all->second.begin(), mtime_all->second.end()));
        auto filename = string(filename_all->second.begin(), filename_all->second.end());
        auto clientid = string(clientid_all->second.begin(), clientid_all->second.end());
        
        string filepath = WrapPath(filename);

        filename_clientid_mutex.lock_shared();
        // check the current filename client id map file
        auto clientid_lock_m = filename_clientid.find(filename);
        string clientid_lock;

        // there is no lock on the file
        if (clientid_lock_m == filename_clientid.end()){

            filename_clientid_mutex.unlock_shared();
            err_message << "No lock on the file" << endl; 
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());

        // current client already put a lock on the file. 
        } else if ((clientid_lock = clientid_lock_m->second).compare(clientid) != 0) {

            filename_clientid_mutex.unlock_shared();
            err_message << "client id " << clientid << " already locks the file" << endl;
            dfs_log(LL_SYSINFO) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        }
        filename_clientid_mutex.unlock_shared();

        filename_write_mutex.lock_shared();
        auto file_access_mutex_all = filename_write_mutex_map.find(filename);
        shared_timed_mutex* file_access_mutex = file_access_mutex_all->second.get();
        filename_write_mutex.unlock_shared();

        dir_mutex.lock();
        file_access_mutex->lock();

        Status result_check_sum = verifyChecksum(metadata, filepath);
        if (!result_check_sum.ok()){
            struct stat result;
            if (result_check_sum.error_code() == StatusCode::ALREADY_EXISTS && stat(filepath.c_str(), &result) == 0 && mtime > result.st_mtime){
                dfs_log(LL_SYSINFO) << "Client mtime is greater than server mtime";
                struct utimbuf new_mtime;
                new_mtime.modtime = mtime;
                if (!utime(filepath.c_str(), &new_mtime)) {
                    dfs_log(LL_SYSINFO) << "Updated mtime for " << filepath;
                } else {
                    dfs_log(LL_ERROR) << "Fail to updatetime for " << filepath;
                }
            }
            filename_clientid_mutex.lock();
            filename_clientid.erase(filename);
            filename_clientid_mutex.unlock();
            file_access_mutex->unlock();
            dir_mutex.unlock();

            dfs_log(LL_ERROR) << result_check_sum.error_message();
            return result_check_sum;
        }

        dfs_log(LL_SYSINFO) << "Start to store the file " << filepath << "on the server";
        FileBytes file_bytes;
        ofstream ofs;
        try {
            while (reader -> Read(&file_bytes)){

                if(!ofs.is_open()){
                    ofs.open(filepath, ios::trunc);
                }

                if (context->IsCancelled()){
                    filename_clientid_mutex.lock();
                    filename_clientid.erase(filename);
                    filename_clientid_mutex.unlock();
                    file_access_mutex->unlock();
                    dir_mutex.unlock();

                    err_message <<  "Deadline exceeded" << endl;
                    dfs_log(LL_ERROR) << err_message.str();
                    return Status(StatusCode::DEADLINE_EXCEEDED, err_message.str());
                }

                ofs << file_bytes.bytes_chunk();
            }
            ofs.close();
            filename_clientid_mutex.lock();
            filename_clientid.erase(filename);
            filename_clientid_mutex.unlock();

        } catch (exception const& err) {
            filename_clientid_mutex.lock();
            filename_clientid.erase(filename);
            filename_clientid_mutex.unlock();
            file_access_mutex->unlock();
            dir_mutex.unlock();

            err_message << "Error in storing file " << err.what() << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        }

        struct stat result;
        if (stat(filepath.c_str(), &result) != 0) {

            file_access_mutex->unlock();
            dir_mutex.unlock();

            err_message << "Fail to retrieve fileinfo of " << filepath << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::NOT_FOUND, err_message.str());
        }
        file_access_mutex->unlock();
        dir_mutex.unlock();

        reply->set_name(filename);
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
        string filepath = WrapPath(file_fetch->name());

        filename_write_mutex.lock();
        if (filename_write_mutex_map.end() == filename_write_mutex_map.find(file_fetch->name())){
            filename_write_mutex_map[file_fetch->name()] = make_unique<shared_timed_mutex>();
        }
        filename_write_mutex.unlock();

        filename_write_mutex.lock_shared();
        auto file_access_mutex_all = filename_write_mutex_map.find(file_fetch->name());
        shared_timed_mutex* file_access_mutex = file_access_mutex_all->second.get();
        filename_write_mutex.unlock_shared();

        file_access_mutex->lock();

        struct stat result;
        stringstream err_message;

        if (stat(filepath.c_str(), &result) != 0){
            file_access_mutex->unlock();

            err_message << filepath << " does not exist" << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::NOT_FOUND, err_message.str());
        }        

        Status result_check_sum = verifyChecksum(context->client_metadata(), filepath);
        if (!result_check_sum.ok()){

            if (result_check_sum.error_code() == StatusCode::ALREADY_EXISTS) {
                const multimap<string_ref, string_ref>& metadata = context->client_metadata();
                auto mtime_all = metadata.find("mtime");
                if (mtime_all == metadata.end()){
                    file_access_mutex->unlock();

                    err_message << "Missing mtime" << endl;
                    dfs_log(LL_ERROR) << err_message.str();
                    return Status(StatusCode::INTERNAL, err_message.str());
                }
                long mtime = stol(string(mtime_all->second.begin(), mtime_all->second.end()));

                if (mtime > result.st_mtime){
                    dfs_log(LL_SYSINFO) << "Updating mtime as Client mtime greater than server mtime";
                    struct utimbuf mtime_new;
                    mtime_new.modtime = mtime;
                    mtime_new.actime = mtime;
                    if (!utime(filepath.c_str(), &mtime_new)) {
                        dfs_log(LL_SYSINFO) << "Updated " << filepath << " mtime to " << mtime;
                    }
                }
            }

            file_access_mutex->unlock();
            dfs_log(LL_ERROR) << result_check_sum.error_message();
            return result_check_sum;
        }
        file_access_mutex->unlock();

        file_access_mutex->lock_shared();

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

                    file_access_mutex->unlock_shared();

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

            file_access_mutex->unlock_shared();

            if (bytes_total != filelen){
                err_message << "The impossible happened" << endl;
                dfs_log(LL_ERROR) << err_message.str();
                return Status(StatusCode::INTERNAL, err_message.str());
            }
        } catch (exception const& err) {
            file_access_mutex->unlock_shared();
            err_message << "Error reading file" <<err.what() << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        }
        dfs_log(LL_SYSINFO) << "Completed fetching " << filepath;
        return Status::OK;
    }

    Status ListAllFiles(
        ServerContext* context,
        const Empty* request,
        Files* reply
    ) override {

        dir_mutex.lock_shared();

        DIR *dir;
        struct dirent *ent;
        stringstream err_message;
        if ((dir = opendir(mount_path.c_str())) == NULL) {
            dir_mutex.unlock_shared();
            /* could not open directory */
            err_message << "Failed to open mountpath " << mount_path << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status::OK;
        }
        /* traverse everything in directory */
        while ((ent = readdir(dir)) != NULL){
            /*
            if (context->IsCancelled()){
                dfs_log(LL_ERROR) << "Request deadline has expired";
                return Status(StatusCode::DEADLINE_EXCEEDED, "Request deadline has expired");
            }*/
            string dirEntry(ent->d_name);
            string filepath = WrapPath(dirEntry);
            FileStatus* rep = reply->add_file();
            stringstream err_message;
            struct stat result;
            if (stat(filepath.c_str(), &result) != 0){

                dir_mutex.unlock_shared();
                err_message << "Getting file info for file" << filepath << " failed with: " << endl;
                dfs_log(LL_ERROR) << err_message.str();
                return Status(StatusCode::NOT_FOUND, err_message.str());
            }
            rep->set_name(dirEntry);
            rep->set_filelen(result.st_size);
            Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_mtime));
            Timestamp* time_created = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_ctime));
            rep->set_allocated_time_modified(time_modified);
            rep->set_allocated_time_created(time_created);

        }
        closedir(dir);
        dir_mutex.unlock_shared();
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

        filename_write_mutex.lock();
        if (filename_write_mutex_map.end() == filename_write_mutex_map.find(request_file->name())){
            filename_write_mutex_map[request_file->name()] = make_unique<shared_timed_mutex>();
        }
        filename_write_mutex.unlock();

        filename_write_mutex.lock_shared();
        auto file_access_mutex_all = filename_write_mutex_map.find(request_file->name());
        shared_timed_mutex* file_access_mutex = file_access_mutex_all->second.get();
        filename_write_mutex.unlock_shared();    

        string filepath = WrapPath(request_file->name());

        file_access_mutex->lock_shared();

        struct stat result;
        stringstream err_message;
        /* Get FileStatus of file*/
        if (stat(filepath.c_str(), &result) != 0){

            file_access_mutex->unlock_shared();

            err_message << "Start to get the info of " << filepath << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::NOT_FOUND, err_message.str());
        }
        
        file_status->set_name(filepath);
        file_status->set_filelen(result.st_size);
        Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_mtime));
        Timestamp* time_created = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_ctime));
        file_status->set_allocated_time_modified(time_modified);
        file_status->set_allocated_time_created(time_created);

        file_access_mutex->unlock_shared();

        return Status::OK;
    }    

    Status DeleteFile(
        ServerContext* context,
        const File* file_delete,
        FileReply* reply
    ) override {
        const string& filepath = WrapPath(file_delete->name());

        const multimap<string_ref, string_ref>& metadata = context->client_metadata();
        //auto filename_all = metadata.find("filename");
        auto clientid_all = metadata.find("clientid");
        //auto mtime_all = metadata.find("mtime");   

        stringstream err_message;
        if (clientid_all == metadata.end()){

            err_message << "No client id on the file" << endl; 
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());

        }         
        auto clientid = string(clientid_all->second.begin(), clientid_all->second.end());

        filename_write_mutex.lock();
        if (filename_write_mutex_map.end() == filename_write_mutex_map.find(file_delete->name())){
            filename_write_mutex_map[file_delete->name()] = make_unique<shared_timed_mutex>();
        }
        filename_write_mutex.unlock();

        filename_write_mutex.lock_shared();
        auto file_access_mutex_all = filename_write_mutex_map.find(file_delete->name());
        shared_timed_mutex* file_access_mutex = file_access_mutex_all->second.get();
        filename_write_mutex.unlock_shared();

        filename_clientid_mutex.lock_shared();
        // check the current filename client id map file
        auto clientid_lock_m = filename_clientid.find(file_delete->name());
        string clientid_lock;

        // there is no lock on the file
        if (clientid_lock_m == filename_clientid.end()){

            filename_clientid_mutex.unlock_shared();
            err_message << "No lock on the file" << endl; 
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());

        // current client already put a lock on the file. 
        } else if ((clientid_lock = clientid_lock_m->second).compare(clientid) != 0) {

            filename_clientid_mutex.unlock_shared();
            err_message << "client id " << clientid << " already locks the file" << endl;
            dfs_log(LL_SYSINFO) << err_message.str();
            return Status(StatusCode::INTERNAL, err_message.str());
        }
        filename_clientid_mutex.unlock_shared();

        dfs_log(LL_SYSINFO) << "Start to delete " << filepath;
        dir_mutex.lock();
        file_access_mutex->lock();
        
        // File doesnt exist
        struct stat result;
        if (stat(filepath.c_str(), &result) != 0){

            filename_clientid_mutex.lock();
            filename_clientid.erase(file_delete->name());
            filename_clientid_mutex.unlock();
            file_access_mutex->unlock();
            dir_mutex.unlock();

            err_message << "File " << filepath << " doesnt exit" << endl;
            dfs_log(LL_ERROR) << err_message.str();
            return Status(StatusCode::NOT_FOUND, err_message.str());
        }
        if (context->IsCancelled()){

            filename_clientid_mutex.lock();
            filename_clientid.erase(file_delete->name());
            filename_clientid_mutex.unlock();
            file_access_mutex->unlock();
            dir_mutex.unlock();

            dfs_log(LL_ERROR) << "Request deadline has expired";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Request deadline has expired");
        }
        // Delete file 
        if (remove(filepath.c_str()) != 0){

            filename_clientid_mutex.lock();
            filename_clientid.erase(file_delete->name());
            filename_clientid_mutex.unlock();
            file_access_mutex->unlock();
            dir_mutex.unlock();

            err_message << "fail to delete" << filepath << endl;
            return Status(StatusCode::INTERNAL, err_message.str());
        }
        filename_clientid_mutex.lock();
        filename_clientid.erase(file_delete->name());
        filename_clientid_mutex.unlock();
        file_access_mutex->unlock();
        dir_mutex.unlock();

        reply->set_name(file_delete->name());
        Timestamp* time_modified = new Timestamp(TimeUtil::TimeTToTimestamp(result.st_mtime));
        reply->set_allocated_time_modified(time_modified);
        dfs_log(LL_SYSINFO) << "Completed deleting file successfully" << filepath;
        return Status::OK;
    }    

    Status CallbackList(
        ServerContext* context,
        const File* file_request,
        Files* reply
    ) override {

        dfs_log(LL_DEBUG2) << "CallbackList for " << file_request->name();
        Empty empty_request;
        return this->ListAllFiles(context, &empty_request, reply);
    }

};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
