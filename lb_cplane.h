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
using loadbalancer::FreeLoadBalancerRequest;
using loadbalancer::FreeLoadBalancerReply;
using loadbalancer::ReserveLoadBalancerRequest;
using loadbalancer::ReserveLoadBalancerReply;
using loadbalancer::LoadBalancerStatusRequest;
using loadbalancer::LoadBalancerStatusReply;
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

    const std::string & getAdminToken()    const;
    const std::string & getInstanceToken() const;
    const std::string & getSessionId()     const;
    const std::string & getName()          const;
    const std::string & getLbId()          const;

    google::protobuf::Timestamp getTimestamp()  const;
    int64_t  getTime()         const;
    int64_t  getLocalTime()    const;

    float   getWeight()           const;
//    float   getFillPercent()      const;
//    float   getPidError()         const;

    const std::string & getIpAddress() const;
    uint32_t getUdpPort()              const;
    uint32_t getPortRange()            const;

//
    bool getIsReady()  const;
    bool getIsActive() const;
    void setIsActive(bool active);


	private:


    // Data from CP (reservation and registration)

    /** Administrative token. */
    std::string adminToken;

    /** LB instance token. */
    std::string instanceToken;

//    sessionToken as well??

    /** Backend's session ID. */
    std::string sessionId;

    /** Backend's name. */
    std::string name;

    /** LB's id. */
    std::string lbId;

    /** Backend's weight in CP relative to the weight of other backends in this LB's schedule density. */
    float weight;

    /** Receiving IP address of backend. */
    std::string  ipAddress;

    /** Receiving UDP port of backend. */
    uint16_t  udpPort;

    /** Receiving UDP port range of backend. */
    uint16_t  portRange;


    // Data for sending state updates to CP ...

    /** Time in milliseconds past epoch that this data was taken by backend. */
    google::protobuf::Timestamp timestamp;

    /** Time in milliseconds past epoch that this data was taken by backend.
     *  Same as timestamp but in different format. */
    int64_t time = 0;

    /** Local time in milliseconds past epoch corresponding to backend time.
     *  Hopefully this takes care of time delays and nodes not setting their clocks properly.
     *  Set locally when SendState msg arrives, this helps find how long ago the backend reported data. */
    int64_t localTime = 0;


    /** Ready to receive more data if true. */
    bool isReady;

    /** Is active (reported its status on time). */
    bool isActive;
};



/** Class used to keep status data for a single client/backend. */
class LbClientStatus {

  public:

    std::string name;
    float fillPercent      = 0.;
    float controlSignal    = 0.;
    uint32_t slotsAssigned = 0;

    /** Time this client's stats were last updated. */
    google::protobuf::Timestamp lastUpdated;

    /** Time in milliseconds past epoch that this data was updated.
     *  Same as "lastUpdated" but in different format. */
    int64_t updateTime;
};



/** Class used to send data from backend (client) to control plane (server). */
class LbControlPlaneClient {
    
    public:     
        
        LbControlPlaneClient(const std::string& cpIP, uint16_t cpPort,
                             const std::string& beIP, uint16_t bePort,
                             PortRange bePortRange,
                             const std::string& _name, const std::string& _token,
                             const std::string& lbId,
                             float _weight, float _setPoint);

        int ReserveLoadBalancer();
        int FreeLoadBalancer() const;
        int LoadBalancerStatus();

      	int Register();
      	int Deregister() const;
        int SendState()  const;

        void update(float fill, float pidErr);

        const std::string & getCpAddr()       const;
        const std::string & getDataAddr()     const;
        const std::string & getName()         const;
        const std::string & getAdminToken()   const;
        const std::string & getSessionToken() const;

        uint16_t  getCpPort()           const;
        uint16_t  getDataPort()         const;

		PortRange getDataPortRange()    const;

		float     getSetPointPercent()  const;
		float     getFillPercent()      const;
        float     getPidError()         const;
        bool      getIsReady()          const;


  
    private:

    /** Object used to call backend's grpc API routines. */
    std::unique_ptr<LoadBalancer::Stub> stub_;

    // Used to reserve control plane

    /** LB's name. */
    std::string lbName;

    /** Time in seconds to reserve the LB. */
    int64_t reservedSec = 60;

    /** Token back from CP. */
    std::string instanceToken;

    /** CP sync data receiving IPv4 address. */
    std::string syncIpAddress;

    /** CP sync data receiving port. */
    uint16_t syncUdpPort;

    /** LB data receiving IPv4 address. */
    std::string dataIpv4Address;

    /** LB data receiving IPv6 address. */
    std::string dataIpv6Address;


    // Used to keep stats on LB clients. Key is name, Va
    std::unordered_map<std::string, LbClientStatus> clientStats;



    // Used to connect to control plane

    /** Control plane's IP address (dotted decimal format). */
    std::string cpAddr = "localhost";
    /** Control plane's grpc port. */
    uint16_t cpPort = 56789;
    /** CP's target name (cpAddr:cpPort). */
    std::string cpTarget;

    /** Token used to register. */
    std::string adminToken;


    // Reply from registration request

    /** Token used to send state and to deregister. */
    std::string sessionToken;
    /** Id used to send state and to deregister. */
    std::string sessionId;
    /** LB's id. */
    std::string lbId;


    /** Client/backend/caller's name. */
    std::string name;



    /** PID loop set point. */
    float setPointPercent;

    /** Backend's weight in CP relative to the weight of other backends in this LB's schedule density. */
    float weight;

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