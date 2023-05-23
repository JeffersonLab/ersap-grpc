//
// Copyright 2023, Jefferson Science Associates, LLC.
// Subject to the terms in the LICENSE file found in the top-level directory.
//
// EPSCI Group
// Thomas Jefferson National Accelerator Facility
// 12000, Jefferson Ave, Newport News, VA 23606
// (757)-269-7100


/**
 * @file Contains code to implement ERSAP backend communication to EJFAT LB's control plane.
 */
#ifndef LB_CONTROL_PLANE_H
#define LB_CONTROL_PLANE_H


#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <mutex>
#include <unordered_map>
#include <chrono>
#include <thread>
#include <atomic>
#include <chrono>
#include <time.h>
#include <unistd.h>

#include <sys/types.h>

#include <google/protobuf/util/time_util.h>




#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#ifdef BAZEL_BUILD
#include "examples/protos/loadbalancer.pb.h"
#include "examples/protos/loadbalancer_enum.grpc.pb.h"
#else
#include "loadbalancer.grpc.pb.h"
#include "loadbalancer_enum.grpc.pb.h"
#endif


using grpc::Channel;
using grpc::ClientContext;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::CompletionQueue;
using grpc::ServerAsyncResponseWriter;

using loadbalancer::BackendReport;
using loadbalancer::RegisterRequest;
using loadbalancer::DeregisterRequest;
using loadbalancer::SendStateRequest;
using loadbalancer::RegisterReply;
using loadbalancer::DeregisterReply;
using loadbalancer::SendStateReply;


//using google::protobuf::util;



// Class to represent a single backend and store its state in the control plane / server.
class BackEnd {

    public:
    
        BackEnd(const RegisterRequest* req);
        
        void update(const SendStateRequest* state);
        void printBackendState() const;

        const std::string & getAuthToken()    const;
        const std::string & getSessionToken() const;
        const std::string & getName()         const;

        google::protobuf::Timestamp getTimestamp()  const;
        int64_t  getTime()         const;
        int64_t  getLocalTime()    const;
        uint32_t getCpus()         const;
        uint32_t getRamBytes()     const;
        uint32_t getBufCount()     const;
        uint32_t getBufSizeBytes() const;

        float   getFillPercent()      const;
        float   getSetPointPercent()  const;
        float   getPidError()         const;

        const std::string & getTargetIP() const;
        uint32_t getTargetPort()          const;
        uint32_t getTargetPortRange()     const;

        bool getIsReady()  const;
        bool getIsActive() const;
        void setIsActive(bool active);

	private:
        
        // Data to return to from control-plane/client

        /** Backend's authentication token. */
        std::string authToken;

        /** Backend's session token. */
        std::string sessionToken;

        /** Backend's name. */
        std::string name;


        /** Time in milliseconds past epoch that this data was taken by backend. */
        google::protobuf::Timestamp timestamp;

        /** Time in milliseconds past epoch that this data was taken by backend.
         *  Same as timestamp but in different format. */
        int64_t time = 0;

        /** Local time in milliseconds past epoch corresponding to backend time.
         *  Hopefully this takes care of time delays and nodes not setting their clocks properly.
         *  Set locally when SendState msg arrives, this helps find how long ago the backend reported data. */
        int64_t localTime = 0;


        /** Backend's cpu count. */
        uint32_t cpus;

        /** Backend's RAM. */
        uint32_t ramBytes;

        /** Number of backend's fifo entries. */
        uint32_t bufCount;
        
        /** Bytes in each backend fifo entry. */
        uint32_t bufSizeBytes;

        
        /** Percent of fifo entries filled with unprocessed data (0-1). */
        float fillPercent;
        
        /** PID loop set point (0-1). */
        float setPointPercent;
        
        /** PID error term in percentage of backend's fifo entries (0 - +/-0.5). */
        float pidError;


        /** Receiving IP address of backend. */
        std::string  targetIP;

        /** Receiving UDP port of backend. */
        uint16_t  targetPort;

        /** Receiving UDP port range of backend. */
        uint16_t  targetPortRange;

        /** Ready to receive more data if true. */
        bool isReady;

        /** Is active (reported its status on time). */
        bool isActive;
};


/** Class implementing logic and data behind the control plane / server's behavior. */
class BackendReportServiceImpl final : public BackendReport::AsyncService {

    public:
        Status SendStateAsync(ServerContext* context, const SendStateRequest* state, ServerAsyncResponseWriter<SendStateReply> *responder,
                              CompletionQueue *cq1, CompletionQueue *cq2, void  *ptr);

        Status SendState  (ServerContext* context, const SendStateRequest* state, SendStateReply* reply);
		Status Register   (ServerContext* context, const RegisterRequest* request, RegisterReply* reply);
		Status DeRegister (ServerContext* context, const DeregisterRequest* request, DeregisterReply* reply);

        std::shared_ptr<std::unordered_map<std::string, BackEnd>> getBackEnds();

        void runServer(uint16_t port, BackendReportServiceImpl *service);
        
    private:

        // Another instance when java is soooo much easier, C++ has no thread-safe containers :(
        // Since the control plane will be accessing this map while potentially multiple threads are
        // writing to it SIMULTANEOUSLY, we'll need to protect its access.
        // Easiest to protect writing into it (only in the SendState, Register, and UnRegister methods
        // above) with a mutex. For the control plane reading it, we can return a copy when asked for
        // it in getBackEnds()
  
        // Store data reported from backends to this server
        std::unordered_map<std::string, BackEnd> data;
        std::mutex map_mutex;
        std::unique_ptr<grpc::ServerCompletionQueue> cq;

};


/** Class client sends data from backend to control plane (server). */
class LbControlPlaneClient {
    
    public:     
        
        LbControlPlaneClient(const std::string& targetIP, uint16_t targetPort, const std::string& _name,
                             uint32_t _bufferSize, uint32_t _bufferCount, float _setPoint);
        
      	int Register();
      	int Deregister() const;
        int SendState()  const;

        void update(float fill, float pidErr);
        
        const std::string & getIP()    const;
        const std::string & getName()  const;

		uint16_t  getPort()             const;
		uint32_t  getBufCount()         const;
		uint32_t  getBufSizeBytes()     const;
		float     getSetPointPercent()  const;
		float     getFillPercent()      const;
        float     getPidError()         const;
        bool      getIsReady()          const;


  
    private:
  
        /** Object used to call backend's grpc API routines. */
        std::unique_ptr<BackendReport::Stub> stub_;

        // Used to connect to backend
        
        /** Control plane's IP address (dotted decimal format). */
        std::string ipAddr = "localhost";
         /** Control plane's grpc port. */
        uint16_t port = 56789;
        /** Target's name (ipaddr:port). */
        std::string target;
        
        // Fixed data to send-to/get-from control plane during registration

        /** Token used to register. */
        std::string authToken;
        /** Token used to send state and to deregister. */
        std::string sessionToken;
        
        /** Client/caller's name. */
        std::string name;
        /** PID loop set point. */
        float setPointPercent;
        /** Number of backend's fifo entries. */
        uint32_t bufCount;
        /** Bytes in each backend fifo entry. */
        uint32_t bufSizeBytes;
        
        // Transcient data to send to control plane

        /** Percent of fifo entries filled with unprocessed data. */
        float fillPercent;
        /** PID error term in percentage of backend's fifo entries. */
        float pidError;
        /** Ready to receive more data or not. */
        bool isReady;
  
};

#endif