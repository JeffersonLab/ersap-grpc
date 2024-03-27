//
// Copyright 2023, Jefferson Science Associates, LLC.
// Subject to the terms in the LICENSE file found in the top-level directory.
//
// EPSCI Group
// Thomas Jefferson National Accelerator Facility
// 12000, Jefferson Ave, Newport News, VA 23606
// (757)-269-7100




#include "lb_cplane.h"

static std::atomic<std::uint64_t>  backendToken{0};
using namespace std::chrono;



		//////////////////
		// BackEnd class
		/////////////////


        /**
         * Constructor.
         * @param req registration request from backend.
         */
        BackEnd::BackEnd(const RegisterRequest* req) {
            adminToken      = req->token();
        	name            = req->name();
        	lbId            = req->lbid();
        	weight          = req->weight();
            ipAddress       = req->ipaddress();
            udpPort         = req->udpport();
            portRange       = req->portrange();
        }


        /**
         * Update values.
         * @state current state used to update this object. 
         */
        void BackEnd::update(const SendStateRequest* state) {

            if (state->has_timestamp()) {
                time = google::protobuf::util::TimeUtil::TimestampToMilliseconds(state->timestamp());
                timestamp = state->timestamp();
            }

            // Now record local time
            localTime   = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

            sessionId    = state->sessionid();

//            fillPercent  = state->fillpercent();
//            pidError     = state->piderror();
//            isReady      = state->isready();
        }


        /** Print out backend status. */
        void BackEnd::printBackendState() const {
        	std::cout << "State of "        << name
                      << " @ t = "          << time
                      << std::endl;
       }


        /** Get the admin token.
         *  @return admin token. */
        const std::string & BackEnd::getAdminToken() const {return adminToken;}

        /** Get the LB's instance token.
         *  @return LB's instance token. */
        const std::string & BackEnd::getInstanceToken() const {return instanceToken;}

        /** Get the backend's session ID.
         *  @return backend's session ID. */
        const std::string & BackEnd::getSessionId() const {return sessionId;}

        /** Get the backend's name.
         *  @return backend's name. */
        const std::string & BackEnd::getName() const {return name;}

        /** Get the LB's id.
         *  @return LB's id. */
        const std::string & BackEnd::getLbId() const {return lbId;}


        /** Get the timestamp of the latest backend's sent data.
         *  @return timestamp of the latest backend's sent data. */
        google::protobuf::Timestamp BackEnd::getTimestamp() const {return timestamp;}

        /** Get the timestamp of the latest backend's sent data.
         *  @return timestamp of the latest backend's sent data. */
        int64_t BackEnd::getTime() const {return time;}

        /** Get the locally set timestamp of the arrival of the latest backend's sent data.
         *  @return locally set timestamp of the arrival of the latest backend's sent data. */
        int64_t BackEnd::getLocalTime() const {return localTime;}


        /** Get the weight of the backend compared to other backends in schedule density.
         *  @return weight of the backend. */
        float BackEnd::getWeight() const {return weight;}


        /** Get the backend's receiving IP address (dot-decimal).
         *  @return backend's receiving IP address. */
        const std::string & BackEnd::getIpAddress() const {return ipAddress;}

        /** Get the backend's receiving UDP port.
         *  @return backend's receiving UDP port. */
        uint32_t BackEnd::getUdpPort() const {return udpPort;}

        /** Get the backend's receiving range of udp ports.
         *  @return backend's receiving range of udp ports. */
        uint32_t BackEnd::getPortRange() const {return portRange;}


        /** Get if backend's ready to receive more data.
        *  @return true if backend's ready to receive more data. */
        bool BackEnd::getIsReady() const {return isReady;}

        /** True if backend is actively sending data updates.
        *  @return true if backend is actively sending data updates. */
        bool BackEnd::getIsActive() const {return isActive;}


        /** True if backend is actively sending data updates.
         *  @param active true if backend is actively sending data updates, else false. */
        void BackEnd::setIsActive(bool active) {isActive = active;}



        
 		/////////////////////////////////
		// LbControlPlaneClient class
		/////////////////////////////////
                
       
        /**
         * Constructor.
         * @param cIp          grpc IP address of control plane (dotted decimal format).
         * @param cPort        grpc port of control plane.
         * @param bIp          data-receiving IP address of this backend client.
         * @param bPort        data-receiving port of this backend client.
         * @param bPortRange   range of data-receiving ports for this backend client.
         * @param cliName      name of this backend.
         * @param token        administration token.
         * @param lbID         LB's id.
         * @param cWeight      weight of this client compared to others in schedule density.
         * @param setPoint     PID loop set point (% of fifo).
         * 
         */
        LbControlPlaneClient::LbControlPlaneClient(
                             const std::string& cIP, uint16_t cPort,
                             const std::string& bIP, uint16_t bPort,
                             PortRange bPortRange,
                             const std::string& cliName, const std::string& token,
                             const std::string& lbID,
                             float cWeight, float setPoint) :
                             
                             cpAddr(cIP), cpPort(cPort), beAddr(bIP), bePort(bPort),
                             beRange(bPortRange), name(cliName), adminToken(token),
                             lbId(lbID), weight(cWeight), setPointPercent(setPoint) {
                
		    cpTarget = cIP + ":" + std::to_string(cPort);
		    stub_ = LoadBalancer::NewStub(grpc::CreateChannel(cpTarget, grpc::InsecureChannelCredentials()));
        }  
        
  
	    /**
         * Update internal state of this object (eventually sent to control plane).
         * @param fill      % of fifo filled
         * @param pidErr    pid error (units of % fifo filled)
         */
        void LbControlPlaneClient::update(float fill, float pidErr) {
        	fillPercent = fill;
        	pidError = pidErr;
        	isReady = true;

        	// This is NOT working
        	//if (fill >= 0.95) {
        	//    isReady = false;
        	//}
        }



        /**
		 * Register this backend with the control plane.
		 * @return 0 if successful, 1 if error in grpc communication
		 */
    	int LbControlPlaneClient::Register() {
		    // Registration message we are sending to server
		    RegisterRequest request;

            request.set_token(adminToken);
            request.set_name(name);
            request.set_lbid(lbId);
            request.set_weight(weight);

		    // Network info for this client
            request.set_ipaddress(beAddr);
            request.set_udpport(bePort);
            request.set_portrange(beRange);

		    // Container for the response we expect from server
		    RegisterReply reply;

		    // Context for the client. It could be used to convey extra information to
		    // the server and/or tweak certain RPC behaviors.
		    ClientContext context;

		    // The actual RPC
		    Status status = stub_->Register(&context, request, &reply);

		    // Act upon its status
		    if (!status.ok()) {
		        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
		        return 1;
		    }

		    // Two things returned from CP
            sessionId    = reply.sessionid();
            sessionToken = reply.token();

		    return 0;
    	}
    	
 
        /**
		 * Unregister this backend with the control plane.
		 * @return 0 if successful, 1 if error in grpc communication
		 */
    	int LbControlPlaneClient::Deregister() const {

    	    // Deregistration message we are sending to server
		    DeregisterRequest request;
            request.set_token(sessionToken);
            request.set_lbid(lbId);
            request.set_sessionid(sessionId);

		    // Container for the response we expect from server
		    DeregisterReply reply;

		    // Context for the client. It could be used to convey extra information to
		    // the server and/or tweak certain RPC behaviors.
		    ClientContext context;

		    // The actual RPC
		    Status status = stub_->Deregister(&context, request, &reply);

		    // Act upon its status
		    if (!status.ok()) {
		        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
		        return 1;
		    }
            return 0;
        }
    	
    	  	
    	
        /**
		 * Send the state of this backend to the control plane.
		 * @return 0 if successful, 1 if error in grpc communication
		 */
    	int LbControlPlaneClient::SendState() const {
		    // Data we are sending to the server.
		    SendStateRequest request;

            request.set_token(sessionToken);
            request.set_lbid(lbId);
            request.set_sessionid(sessionId);

            // Set the time
            struct timespec t1;
            clock_gettime(CLOCK_REALTIME, &t1);
            auto timestamp = new google::protobuf::Timestamp{};
            timestamp->set_seconds(t1.tv_sec);
            timestamp->set_nanos(t1.tv_nsec);
            // Give ownership of object to protobuf
            request.set_allocated_timestamp(timestamp);

            request.set_fillpercent(fillPercent);
            request.set_controlsignal(pidError);

            // In order NOT to throw the CP into unstable behavior,
            // always say the we are ready to receive data.
            //request.set_isready(isReady);
            request.set_isready(true);

		    // Container for the data we expect from the server.
		    SendStateReply reply;

		    // Context for the client. It could be used to convey extra information to
		    // the server and/or tweak certain RPC behaviors.
		    ClientContext context;

		    // The actual RPC.
            Status status = stub_->SendState(&context, request, &reply);

		    // Act upon its status.
		    if (!status.ok()) {
		        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
		        return 1;
		    }

		    return 0;
		}
		
  
		// Getters
        const std::string & LbControlPlaneClient::getCpAddr()       const   {return cpAddr;}
        const std::string & LbControlPlaneClient::getDataAddr()     const   {return beAddr;}
        const std::string & LbControlPlaneClient::getName()         const   {return name;}
        const std::string & LbControlPlaneClient::getAdminToken()   const   {return adminToken;}
        const std::string & LbControlPlaneClient::getSessionToken() const   {return sessionToken;}

        uint16_t   LbControlPlaneClient::getCpPort()             const   {return cpPort;}
        uint16_t   LbControlPlaneClient::getDataPort()           const   {return bePort;}

		PortRange  LbControlPlaneClient::getDataPortRange()      const   {return beRange;}

		float      LbControlPlaneClient::getSetPointPercent()    const   {return setPointPercent;}
		float      LbControlPlaneClient::getFillPercent()        const   {return fillPercent;}
        float      LbControlPlaneClient::getPidError()           const   {return pidError;}

        bool       LbControlPlaneClient::getIsReady()            const   {return isReady;}



        /////////////////////////////////
        // LbReservation class
        /////////////////////////////////


        /**
         * Constructor.
         * @param cIp       grpc IP address of control plane (dotted decimal format).
         * @param cPort     grpc port of control plane.
         * @param name      name of LB being reserved.
         * @param token     administration token.
         * @param until     seconds since epoch until which to reserve the LB.
         *
         */
        LbReservation::LbReservation (const std::string& cpIP, uint16_t cpPort,
                                      const std::string& name,
                                      const std::string& token,
                                      int64_t until) :

                cpAddr(cIP), cpPort(cPort), lbName(name),
                adminToken(token), untilSeconds(until) {

            cpTarget = cIP + ":" + std::to_string(cPort);
            stub_ = LoadBalancer::NewStub(grpc::CreateChannel(cpTarget, grpc::InsecureChannelCredentials()));
        }


        /**
         * Reserve a specified LB to use.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbReservation::ReserveLoadBalancer() {
            // Reserve-LB message we are sending to server
            ReserveLoadBalancerRequest request;

            request.set_token(adminToken);
            request.set_name(lbName);

            // Set the time for this reservation to run out
            auto timestamp = new google::protobuf::Timestamp{};
            timestamp->set_seconds(untilSeconds);
            timestamp->set_nanos(0);
            // Give ownership of object to protobuf
            request.set_allocated_until(timestamp);

            // Container for the response we expect from server
            ReserveLoadBalancerReply reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context;

            // The actual RPC
            Status status = stub_->ReserveLoadBalancer(&context, request, &reply);

            // Act upon its status
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            // things returned from CP
            instanceToken   = reply.token();
            lbId            = reply.lbid();
            syncIpAddress   = reply.syncipaddress();
            syncUdpPort     = reply.syncudpport();
            dataIpv4Address = reply.dataipv4address();
            dataIpv6Address = reply.dataipv6address();

            return 0;
        }


        /**
         * Free the LB from a single reserved slot.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbReservation::FreeLoadBalancer() const {

            // Free-LB message we are sending to server
            FreeLoadBalancerRequest request;
            request.set_token(instanceToken);
            request.set_lbid(lbId);

            // Container for the response we expect from server
            FreeLoadBalancerReply reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context;

            // The actual RPC
            Status status = stub_->FreeLoadBalancer(&context, request, &reply);

            // Act upon its status
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }
            return 0;
        }



        /**
         * Get LB status.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbReservation::LoadBalancerStatus() {
            // LB-request-for-status message we are sending to server
            LoadBalancerStatusRequest request;

            request.set_token(adminToken);
            request.set_lbid(lbId);

            // Container for the response we expect from server
            LoadBalancerStatusReply reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context;

            // The actual RPC
            Status status = stub_->LoadBalancerStatus(&context, request, &reply);

            // Act upon its status
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            // Things returned from CP

            // How many clients on this LB?
            int clientCount = reply.workers_size();

            for (size_t j = 0; j < clientCount; j++) {
                std::string name = reply.workers(j).name();

                // Either returns the entry at this key, or creates one if none exists
                auto & stats = clientStats[name];
                stats.fillPercent   = reply.workers(j).fillpercent();
                stats.controlSignal = reply.workers(j).controlsignal();
                stats.slotsAssigned = reply.workers(j).slotsassigned();
                stats.lastUpdated   = reply.workers(j).lastupdated();
                stats.updateTime = google::protobuf::util::TimeUtil::TimestampToMilliseconds(stats.lastUpdated);
            }

            return 0;
        }



        // Getters
        const std::string & LbReservation::getLbName()        const   {return lbName;}
        const std::string & LbReservation::getAdminToken()    const   {return adminToken;}
        const std::string & LbReservation::getInstanceToken() const   {return instanceToken;}
        const std::string & LbReservation::getLbId()          const   {return lbId;}

        const std::string & LbReservation::getCpAddr()        const   {return cpAddr;}
        const std::string & LbReservation::getSyncAddr()      const   {return syncIpAddress;}
        const std::string & LbReservation::getDataAddrV4()    const   {return dataIpv4Address;}
        const std::string & LbReservation::getDataAddrV6()    const   {return dataIpv6Address;}

        uint16_t   LbReservation::getSyncPort() const   {return syncUdpPort;}
        uint16_t   LbReservation::getCpPort()   const   {return cpPort;}
        uint16_t   LbReservation::getDataPort() const   {return 19522;}
         int64_t   LbReservation::getUntil()    const   {return untilSeconds;}
         
        bool  LbReservation::reserved() const  {return closed || timeElapsed;}
        const std::unordered_map<std::string, LbClientStatus> & LbReservation::getClientStats() const {return clientStats;}




