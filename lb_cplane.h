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
#include <regex>
#include <iomanip>
#include <ctime>

#include <cstring>
#include <netdb.h>
#include <arpa/inet.h>


#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/tls_credentials_options.h>
#include <openssl/ssl.h>


#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>


#ifdef __APPLE__
    #include <sys/sysctl.h>
#endif

#include <google/protobuf/util/time_util.h>

#ifdef BAZEL_BUILD
#include "examples/protos/loadbalancer.pb.h"
#else
#include "loadbalancer.grpc.pb.h"
#endif

#ifdef USE_STANDALONE_ASIO
    #include "asio.hpp"
#else
    #include "boost/asio.hpp"
#endif
#include <boost/system/system_error.hpp>

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

using loadbalancer::ReserveLoadBalancerRequest;
using loadbalancer::ReserveLoadBalancerReply;
using loadbalancer::GetLoadBalancerRequest;

using loadbalancer::LoadBalancerStatusRequest;
using loadbalancer::LoadBalancerStatusReply;

using loadbalancer::FreeLoadBalancerRequest;
using loadbalancer::FreeLoadBalancerReply;

using loadbalancer::AddSendersRequest;
using loadbalancer::AddSendersReply;

using loadbalancer::RemoveSendersRequest;
using loadbalancer::RemoveSendersReply;

using loadbalancer::RegisterRequest;
using loadbalancer::RegisterReply;

using loadbalancer::DeregisterRequest;
using loadbalancer::DeregisterReply;

using loadbalancer::SendStateRequest;
using loadbalancer::SendStateReply;

using loadbalancer::OverviewRequest;
using loadbalancer::OverviewReply;
using loadbalancer::Overview;

using loadbalancer::VersionRequest;
using loadbalancer::VersionReply;





//------------------------------------------------------------------------------------


/** Class used to send data from backend (client) to control plane (server). */
class LbControlPlaneClient {
    
    public:

        LbControlPlaneClient(const std::string& cpIP, uint16_t cpPort,
                             const std::string& beIP, uint16_t bePort,
                             PortRange bePortRange,
                             const std::string& name, const std::string& token,
                             const std::string& lbId,
                             float weight, float minFactor, float maxFactor);
//    float weight = 1.F, float minFactor = 0.F, float maxFactor = 0.F);

      	int Register();
      	int Deregister() const;
        int SendState()  const;

        void update(float fill, float pidErr, bool isReady = true);

        const std::string & getCpAddr()       const;
        const std::string & getDataAddr()     const;
        const std::string & getName()         const;
        const std::string & getToken()   const;
        const std::string & getSessionToken() const;

        uint16_t  getCpPort()           const;
        uint16_t  getDataPort()         const;

		PortRange getDataPortRange()    const;

		float     getFillPercent()      const;
        float     getPidError()         const;
        bool      getIsReady()          const;



private:

    /** Object used to call backend's grpc API routines. */
    std::unique_ptr<LoadBalancer::Stub> _stub;

    /** Control plane's IP address (dotted decimal format). */
    std::string cpAddr;

    /** Control plane's grpc port. */
    uint16_t cpPort;


    // Used to register with control plane

    /** This backend client's data-receiving IP addr. */
    std::string beAddr;

    /** This backend client's data-receiving port. */
    uint16_t bePort;

    /** This backend client's data-receiving port range. */
    PortRange beRange;

    /** Client/backend/caller's name. */
    std::string name;

    /** Token (either admin or instance) used to register. */
    std::string token;

    /** LB's id. */
    std::string lbId;

    /** Backend's weight in CP relative to the weight of other
     * backends in this LB's schedule density. */
    float weight;


    /** This factor is multiplied with the number of scheduling slots that
     *  would be assigned evenly, to determine min number of slots. For example,
     *  4 nodes with a minFactor of 0.5 = (512 slots / 4) * 0.5 = min 64 slots. */
    float minFactor;

    /** This factor is multiplied with the number of scheduling slots that
     *  would be assigned evenly, to determine max number of slots. For example,
     *  4 nodes with a maxFactor of 2 = (512 slots / 4) * 2 = max 256 slots.
     *  Set to 0 to specify no maximum. */
    float maxFactor;



    // Reply from registration request

    /** Token used to send state and to deregister. */
    std::string sessionToken;

    /** Id used to send state and to deregister. */
    std::string sessionId;



    // Transient data to send to control plane

    /** Percent of fifo entries filled with unprocessed data. */
    float fillPercent;

    /** PID error term in percentage of backend's fifo entries. */
    float pidError;

    /** Ready to receive more data or not. */
    bool isReady = true;

};


//------------------------------------------------------------------------------------


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

    /** Human readable date and time of updateTime. */
    std::string updateTimeString;


    void printClientStats(std::ostream& out, std::string& indent) const {
        out << indent << "name           : " << name << std::endl;
        out << indent << "fill %         : " << fillPercent << std::endl;
        out << indent << "control sig    : " << controlSignal << std::endl;
        out << indent << "slots assigned : " << slotsAssigned << std::endl;
        out << indent << "update time    : " << updateTimeString << std::endl;
    }

};


//------------------------------------------------------------------------------------


/** Class used to keep status data for a single reserved LB in a CP. */
class LdBalancer {

public:

    friend class CpOverview;
    friend class LbAdmin;


    // Getters
    const std::string & getUri4()          const   {return uri4;}
    const std::string & getUri6()          const   {return uri6;}

    const std::string & getName()          const   {return name;}
    const std::string & getInstanceToken() const   {return instanceToken;}
    const std::string & getLbId()          const   {return lbId;}

    const std::string & getSyncAddr()      const   {return syncIpAddress;}
    const std::string & getDataAddrV4()    const   {return dataIpv4Address;}
    const std::string & getDataAddrV6()    const   {return dataIpv6Address;}

    uint16_t   getSyncPort() const   {return syncUdpPort;}
    uint16_t   getDataPort() const   {return 19522;}
    int64_t     getUntil()   const   {return untilSeconds;}
    uint32_t   getFpgaLbId() const   {return fpgaLbId;}

    bool reservationElapsed() const {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);

        if (now.tv_sec > untilSeconds) {
            return true;
        }
        return false;
    };

    const std::set<std::string> & getSenders() const {return curSenders;}


    // From LoadBalancerStatusReply msg
    int64_t  getExpiresAt()    const {return expireAtMilliSeconds;}
    uint64_t getCurrentEpoch() const {return curEpoch;}
    uint64_t getPredictedEventNumber() const {return curPredictedEventNum;}
    const std::set<std::string> & getCurSenders() const {return curSenders;}
    const std::unordered_map<std::string, LbClientStatus> &
                    getClientStats() const {return clientStats;}


    void printLbStats(std::ostream& out, std::string& indent) const {

        std::string subLbIndent  = indent + "  ";
        std::string workerIndent = indent + "    ";


        out << indent << "LB_id " << lbId << " (name = " << name << "):" << std::endl;
        out << subLbIndent << "fpga id     : " << fpgaLbId << std::endl;
        out << subLbIndent << "token       : " << instanceToken << std::endl;
        out << subLbIndent << "sync addr   : " << syncIpAddress << std::endl;
        out << subLbIndent << "sync port   : " << syncUdpPort << std::endl;

        if (!dataIpv4Address.empty()) {
            out << subLbIndent << "data addr 4 : " << dataIpv4Address << std::endl;
            out << subLbIndent << "uri (ipv4)  : " << uri4 << std::endl;
        }
        if (!dataIpv6Address.empty()) {
            out << subLbIndent << "data addr 6 : " << dataIpv6Address << std::endl;
            out << subLbIndent << "uri (ipv6)  : " << uri6 << std::endl;
        }


        out << subLbIndent << "epoch       : " << curEpoch << std::endl;
        out << subLbIndent << "predict ev# : " << curPredictedEventNum << std::endl;
        out << subLbIndent << "            : " << std::hex << std::showbase << curPredictedEventNum << std::dec << std::endl;
        out << subLbIndent << "expire at   : " << expireAtString << std::endl;
        out << subLbIndent << "            : " << std::hex << std::showbase << expireAtMilliSeconds << std::dec << std::endl;
        out << subLbIndent << "update time : " << timeSent << std::endl << std::endl;

        out << subLbIndent << "senders : " << std::endl;
        for (const std::string sender : curSenders) {
            out << workerIndent << sender << std::endl;
        }

        out << std::endl;

        out << subLbIndent << "clients : " << std::endl;
        for (const auto& workPair : clientStats) {
            const LbClientStatus &stats = workPair.second;
            stats.printClientStats(out, workerIndent);
            out << std::endl;
        }
    }



private:

    std::string name;

    /** Time LB reservation will run out. */
    google::protobuf::Timestamp until;

    /** Time in seconds past epoch that LB reservation will run out.
     *  Same as "until" but in different format. */
    int64_t untilSeconds;

    /** Contains data senders given in the reserve-LB command. Not really used. */
    std::set<std::string> senders;

    /** Construct the IPv4 uri for reference. */
    std::string uri4;

    /** Construct the IPv6 uri for reference. */
    std::string uri6;



    // Reserve reply

    /** CP sync data receiving IPv4 address. */
    std::string syncIpAddress;

    /** CP sync data receiving port. */
    uint16_t syncUdpPort;

    /** LB data receiving IPv4 address. */
    std::string dataIpv4Address;

    /** LB data receiving IPv6 address. */
    std::string dataIpv6Address;

    /** LB's id. */
    std::string lbId;

    /** Token back from CP for LB reservation. */
    std::string instanceToken;

    /** FPGA LB ID, for use in correlating logs/metrics. */
    uint32_t fpgaLbId;


    // Status reply

    /** Epoch currently in use. */
    uint64_t curEpoch;

    /** Next predicted event #. */
    uint64_t curPredictedEventNum;

    /** Time LB reservation will expire. */
    google::protobuf::Timestamp expiresAt;

    /** "expiresAt" in milliseconds past epoch. */
    int64_t expireAtMilliSeconds;

    /** Human readable date and time of expiration (second resolution) . */
    std::string expireAtString;

    /** Human readable date and time of time reply sent . */
    std::string timeSent;

    /** Contains data senders currently recognized by CP. */
    std::set<std::string> curSenders;

    /** Map used to store stats on LB clients.
     * Key is client name, val is LbClientStatus struct. */
    std::unordered_map<std::string, LbClientStatus> clientStats;
};


//------------------------------------------------------------------------------------


/**
 * A single instance of this class is used to represent the
 * state of a control plane and all the load balancers it contains.
 */
class CpOverview {

    friend class LdBalancer;

public:

    CpOverview(const std::string& cpIP, uint16_t cpPort, const std::string& token);
    int getUpdate();
    int getVersion();
    void printCpStats(std::ostream& out, std::string& indent) const;

private:

    /** Control plane's IP address (dotted decimal format). */
    std::string cpAddr;

    /** Control plane's grpc port. */
    uint16_t cpPort;

    /** CP version. */
    std::string version;

    /** Object used to call backend's grpc API routines. */
    std::unique_ptr<LoadBalancer::Stub> _stub;

    /** Token used to reserve LB. */
    std::string adminToken;


    /** Key = lbID, Val = LBalancer obj. */
    std::unordered_map<std::string, LdBalancer> lbStats;
};


//------------------------------------------------------------------------------------


/**
 * A single instance of this class is used to represent and
 * interact with a single load balancer.
 * The static methods act on the specified LB.
 */
class LbAdmin {

public:

    static std::string ReserveLoadBalancer(const std::string& cpIP, uint16_t cpPort,
                                           const std::string& lbName,
                                           const std::string& adminToken,
                                           const std::set<std::string> &senders,
                                           int64_t untilSeconds, bool ipv6);

    static int FreeLoadBalancer(const std::string& cpIP, uint16_t cpPort,
                                const std::string& lbId,
                                const std::string& adminToken);

    static int LoadBalancerStatus(const std::string& cpIP, uint16_t cpPort,
                                  const std::string& lbId,
                                  const std::string& adminToken,
                                  std::unordered_map<std::string, LbClientStatus>& stats);

    static std::string GetLbUri(const std::string& cpIP, uint16_t cpPort,
                                const std::string& lbId,
                                const std::string& adminToken,
                                bool useIPv6);

    static int AddSenders(const std::string& cpIP, uint16_t cpPort,
                          const std::string& lbId,
                          const std::string& adminToken,
                          const std::set<std::string> &senders);

    static int RemoveSenders(const std::string& cpIP, uint16_t cpPort,
                             const std::string& lbId,
                             const std::string& adminToken,
                             const std::set<std::string> &senders);



    LbAdmin(const std::string& cpIP, uint16_t cpPort,
                  const std::string& adminToken);

    int ReserveLoadBalancer(const std::string& name,
                            const std::set<std::string> &senders,
                            int64_t until);
    int FreeLoadBalancer();
    int LoadBalancerStatus();
    int AddSenders(const std::set<std::string> &senders);
    int RemoveSenders(const std::set<std::string> &senders);

    const std::string & getAdminToken()  const;
    const std::string & getUri4()        const;
    const std::string & getUri6()        const;
    const std::string & getCpAddr()      const;
    uint16_t            getCpPort()      const;
    bool reserved() const;




private:


    /** Does this object represent a current LB reservation?
     *  Or has it expired or been terminated? */
    bool isReserved = false;

    /** Object used to call backend's grpc API routines. */
    std::unique_ptr<LoadBalancer::Stub> _stub;

    /** Control plane's IP address (dotted decimal format). */
    std::string cpAddr;

    /** Control plane's grpc port. */
    uint16_t cpPort;

    /** Token used to reserve LB. */
    std::string adminToken;

    /** URI IPv6 for using this LB. */
    std::string uri6;

    /** URI IPv4 for using this LB. */
    std::string uri4;



    /** Reserved LB. */
    LdBalancer lb;

};


#endif