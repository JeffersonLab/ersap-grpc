//
// Copyright 2023, Jefferson Science Associates, LLC.
// Subject to the terms in the LICENSE file found in the top-level directory.
//
// EPSCI Group
// Thomas Jefferson National Accelerator Facility
// 12000, Jefferson Ave, Newport News, VA 23606
// (757)-269-7100


/**
* @file
* This file contains code to implement an ERSAP backend communication to an EJFAT LB's control plane.
*
* It contains the BackEnd class is a simple class to hold and modify data.
*
* It contains the LoadBalancerServiceImpl class which acts as a simulated control plane.
* It is setup to do synchronous communication with the backend. It defines commands that
* handle the backend's call to invoke an action on the server such as: Register, SendState, and Deregister.
* It also defines the runServer method which implements these functions in a grpc server.
*
* Finally, it contains the LbControlPlaneClient class which is used by a backend in order
* to communicate with a simulated (or perhaps a real) control plane server. It allows the
* backend to Register, SendState, and Deregister as well as control the state that it sends.
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

#ifdef __APPLE__
    #include <sys/sysctl.h>
#endif

#include <google/protobuf/util/time_util.h>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#ifdef BAZEL_BUILD
#include "examples/protos/loadbalancer.pb.h"
#else
#include "loadbalancer.grpc.pb.h"
#endif


using grpc::Channel;
using grpc::ClientContext;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::CompletionQueue;
using grpc::ServerAsyncResponseWriter;

using loadbalancer::PortRange;
using loadbalancer::LoadBalancer;
using loadbalancer::RegisterRequest;
using loadbalancer::DeregisterRequest;
using loadbalancer::SendStateRequest;
using loadbalancer::RegisterReply;
using loadbalancer::DeregisterReply;
using loadbalancer::SendStateReply;


//using google::protobuf::util;



/** Class to represent a single backend and store its state in the control plane / server. */
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


/** Class implementing logic and data behind a simulated control plane / server's behavior. */
//class LoadBalancerServiceImpl final : public LoadBalancer::AsyncService {
class LoadBalancerServiceImpl final : public LoadBalancer::Service {

    public:
//        Status SendStateAsync(ServerContext* context, const SendStateRequest* state, ServerAsyncResponseWriter<SendStateReply> *responder,
//                              CompletionQueue *cq1, CompletionQueue *cq2, void  *ptr);
        Status SendState  (ServerContext* context, const SendStateRequest* state, SendStateReply* reply);
		Status Register   (ServerContext* context, const RegisterRequest* request, RegisterReply* reply);
		Status DeRegister (ServerContext* context, const DeregisterRequest* request, DeregisterReply* reply);

        std::shared_ptr<std::unordered_map<std::string, BackEnd>> getBackEnds();

        void runServer(uint16_t port, LoadBalancerServiceImpl *service);
        
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
//        std::unique_ptr<grpc::ServerCompletionQueue> cq;

};


/** Class used to send data from backend (client) to control plane (server). */
class LbControlPlaneClient {
    
    public:     
        
        LbControlPlaneClient(const std::string& cpIP, uint16_t cpPort,
                             const std::string& beIP, uint16_t bePort,
                             PortRange bePortRange,
                             const std::string& _name, const std::string& _token,
                             uint32_t _bufferSize, uint32_t _bufferCount, float _setPoint);
        
      	int Register();
      	int Deregister() const;
        int SendState()  const;

        void update(float fill, float pidErr);

        const std::string & getCpAddr()    const;
        const std::string & getDataAddr()  const;
        const std::string & getName()      const;
        const std::string & getToken()     const;

        uint16_t  getCpPort()           const;
        uint16_t  getDataPort()         const;
		uint32_t  getBufCount()         const;
		uint32_t  getBufSizeBytes()     const;

		PortRange getDataPortRange()    const;

		float     getSetPointPercent()  const;
		float     getFillPercent()      const;
        float     getPidError()         const;
        bool      getIsReady()          const;


  
    private:
  
        /** Object used to call backend's grpc API routines. */
        std::unique_ptr<LoadBalancer::Stub> stub_;

        // Used to connect to control plane
        
        /** Control plane's IP address (dotted decimal format). */
        std::string cpAddr = "localhost";
         /** Control plane's grpc port. */
        uint16_t cpPort = 56789;
        /** CP's target name (cpAddr:cpPort). */
        std::string cpTarget;
        
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

        /** This backend client's data-receiving IP addr. */
        std::string beAddr;
        /** This backend client's data-receiving port. */
        uint16_t bePort;
        /** This backend client's data-receiving port range. */
        PortRange beRange;

        // Transient data to send to control plane

        /** Percent of fifo entries filled with unprocessed data. */
        float fillPercent;
        /** PID error term in percentage of backend's fifo entries. */
        float pidError;
        /** Ready to receive more data or not. */
        bool isReady;

};

#endif