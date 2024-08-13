//
// Copyright 2023, Jefferson Science Associates, LLC.
// Subject to the terms in the LICENSE file found in the top-level directory.
//
// EPSCI Group
// Thomas Jefferson National Accelerator Facility
// 12000, Jefferson Ave, Newport News, VA 23606
// (757)-269-7100




#include "lb_cplane.h"

using namespace std::chrono;




        /**
         * Create grpc stub object.
         * @param cpIP  grpc IP address of control plane (dotted decimal format).
         * @param cPort grpc port of control plane.
         *
         */
        static std::unique_ptr<LoadBalancer::Stub>
                createStub (const std::string& cpIP, uint16_t cpPort) {

            std::string cpTarget = cpIP + ":" + std::to_string(cpPort);

            // Disable most of server certificate validation
            grpc::experimental::TlsChannelCredentialsOptions topts;
            std::shared_ptr<grpc::experimental::NoOpCertificateVerifier> verifier =
                    std::make_shared<grpc::experimental::NoOpCertificateVerifier>();
            topts.set_verify_server_certs(false);
            topts.set_check_call_host(false);
            topts.set_certificate_verifier(verifier);

            auto channel = grpc::CreateChannel(cpTarget, grpc::experimental::TlsCredentials(topts));
            auto stub = LoadBalancer::NewStub(channel);

            return stub;
        }


        
 		/////////////////////////////////
		// LbControlPlaneClient class
		/////////////////////////////////


        /**
         * Constructor.
         * @param cpIp         grpc IP address of control plane (dotted decimal format).
         * @param cpPort       grpc port of control plane.
         * @param beIp         data-receiving IP address of this backend client.
         * @param bePort       data-receiving port of this backend client.
         * @param beRange      range of data-receiving ports for this backend client.
         * @param cliName      name of this backend.
         * @param token        administration or instance token.
         * @param lbId         LB's id.
         * @param weight       weight of this client compared to others in schedule density.
         * @param minFactor    multiplicative factor used to set min number of scheduling slots.
         * @param maxFactor    multiplicative factor used to set max number of scheduling slots.
         *
         */
        LbControlPlaneClient::LbControlPlaneClient(
                const std::string& cpIP, uint16_t cpPort,
                const std::string& beIP, uint16_t bePort,
                PortRange beRange,
                const std::string& cliName, const std::string& token,
                const std::string& lbId,
                float weight, float minFactor, float maxFactor) :

                cpAddr(cpIP), cpPort(cpPort), beAddr(beIP), bePort(bePort),
                beRange(beRange), name(cliName), token(token), lbId(lbId),
                weight(weight), minFactor(minFactor), maxFactor(maxFactor) {

            _stub = createStub(cpIP, cpPort);
        }





        /**
         * Update internal state of this object (eventually sent to control plane).
         * @param fill      % of fifo filled
         * @param pidErr    pid error (units of % fifo filled)
         * @param ready     if true, ready for more data
         */
        void LbControlPlaneClient::update(float fill, float pidErr, bool ready) {
        	fillPercent = fill;
        	pidError = pidErr;
        	isReady = ready;
        }



        /**
		 * Register this backend with the control plane.
		 * @return 0 if successful, 1 if error in grpc communication
		 */
    	int LbControlPlaneClient::Register() {
		    // Registration message we are sending to server
		    RegisterRequest request;

            request.set_lbid(lbId);
            request.set_name(name);
            request.set_weight(weight);
            request.set_minfactor(minFactor);
            request.set_maxfactor(maxFactor);

		    // Network info for this client
            request.set_ipaddress(beAddr);
            request.set_udpport(bePort);
            request.set_portrange(beRange);

		    // Container for the response we expect from server
		    RegisterReply reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            // Set bearer token in header.
            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + token);

		    // The actual RPC
		    Status status = _stub->Register(&context, request, &reply);

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
            request.set_lbid(lbId);
            request.set_sessionid(sessionId);

		    // Container for the response we expect from server
		    DeregisterReply reply;

            // Set bearer token in header.
            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + token);

		    // The actual RPC
		    Status status = _stub->Deregister(&context, request, &reply);

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
            request.set_isready(isReady);

		    // Container for the data we expect from the server.
		    SendStateReply reply;

            // Set bearer token in header.
            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + token);

		    // The actual RPC.
            Status status = _stub->SendState(&context, request, &reply);

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
        const std::string & LbControlPlaneClient::getToken()        const   {return token;}
        const std::string & LbControlPlaneClient::getSessionToken() const   {return sessionToken;}

        uint16_t   LbControlPlaneClient::getCpPort()                const   {return cpPort;}
        uint16_t   LbControlPlaneClient::getDataPort()              const   {return bePort;}

		PortRange  LbControlPlaneClient::getDataPortRange()         const   {return beRange;}

		float      LbControlPlaneClient::getFillPercent()           const   {return fillPercent;}
        float      LbControlPlaneClient::getPidError()              const   {return pidError;}

        bool       LbControlPlaneClient::getIsReady()               const   {return isReady;}



        /////////////////////////////////
        // CpOverview class
        /////////////////////////////////


        /**
        * Constructor.
        * @param cIp       grpc IP address of control plane (dotted decimal format).
        * @param cPort     grpc port of control plane.
        * @param token     administration token.
        *
        */
        CpOverview::CpOverview (const std::string& cpIP, uint16_t cpPort,
                                const std::string& token) :

                cpAddr(cpIP), cpPort(cpPort), adminToken(token) {

            _stub = createStub(cpIP, cpPort);
        }


        /**
         * Get the version of the current CP.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int CpOverview::getVersion() {

            // Get-version message we are sending to server
            VersionRequest request;
            VersionReply reply;

            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            Status status = _stub->Version(&context, request, &reply);
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            version = reply.commit();

            return 0;
        }


        /**
         * Print out all info obtained from the CP.
         */
        void CpOverview::printCpStats(std::ostream& out, std::string& indent) const {
            if (lbStats.empty()) {
                out << indent << "No LBs in this CP" << std::endl << std::endl;
                return;
            }

            out << indent << "CP @ " << cpAddr << ":" << cpPort << std::endl << std::endl;

            // indents
            std::string lbIndent     = indent + "  ";
            std::string subLbIndent  = indent + "    ";
            std::string workerIndent = indent + "      ";

            // For each LB in this CP ...
            for (const auto& pair : lbStats) {
                const LdBalancer &lb = pair.second;
                lb.printLbStats(out, lbIndent);
                out << std::endl;
            }
            out << std::endl;
        }




        /**
         * Get overview of entire CP.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int CpOverview::getUpdate() {
            // Overview request message we are sending to server
            OverviewRequest request;
            OverviewReply reply;

            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            Status status = _stub->Overview(&context, request, &reply);

            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            //----------------------------
            // Things returned from CP

            lbStats.clear();

            std::stringstream ss;

            // How many LBs?
            int lbCount = reply.loadbalancers_size();

            // For each LB in this CP ...
            for (int j = 0; j < lbCount; j++) {
                std::string name = reply.loadbalancers(j).name();

                auto res = reply.loadbalancers(j).reservation();

                LdBalancer lb;

                lb.name = name;
                lb.instanceToken   = res.token();
                lb.lbId            = res.lbid();
                lb.syncIpAddress   = res.syncipaddress();
                lb.syncUdpPort     = res.syncudpport();
                lb.dataIpv4Address = res.dataipv4address();
                lb.dataIpv6Address = res.dataipv6address();
                lb.fpgaLbId        = res.fpgalbid();


                // Construct the uri's

                if (!lb.dataIpv6Address.empty()) {
                    ss.str("");  // Clear the content
                    ss.clear();  // Reset the error state

                    ss << "ejfat://" << lb.instanceToken << "@" << cpAddr << ":" << cpPort;
                    ss << "/lb/" << lb.lbId;
                    ss << "?data=" << lb.dataIpv6Address << ":19522";
                    ss << "&sync=" << lb.syncIpAddress << ":" << lb.syncUdpPort;
                    lb.uri6 = ss.str();
                }

                if (!lb.dataIpv4Address.empty()) {
                    ss.str("");  // Clear the content
                    ss.clear();  // Reset the error state

                    ss << "ejfat://" << lb.instanceToken << "@" << cpAddr << ":" << cpPort;
                    ss << "/lb/" << lb.lbId;
                    ss << "?data=" << lb.dataIpv4Address << ":19522";
                    ss << "&sync=" << lb.syncIpAddress << ":" << lb.syncUdpPort;
                    lb.uri4 = ss.str();
                }


                auto status = reply.loadbalancers(j).status();

                lb.curEpoch = status.currentepoch();
                lb.curPredictedEventNum = status.currentpredictedeventnumber();



                lb.expiresAt = status.expiresat();
                lb.expireAtMilliSeconds = google::protobuf::util::TimeUtil::TimestampToMilliseconds(lb.expiresAt);

                // Convert msec to seconds
                time_t seconds = lb.expireAtMilliSeconds / 1000;

                // Create a struct to hold the local time
                struct tm *local_time = localtime(&seconds);

                ss.str("");  // Clear the content
                ss.clear();  // Reset the error state

                // The formatted date and time
                ss << std::put_time(local_time, "%Y-%m-%d %H:%M:%S");
                lb.expireAtString = ss.str();



                // Time reply sent
                seconds = google::protobuf::util::TimeUtil::TimestampToMilliseconds(status.timestamp()) / 1000;
                local_time = localtime(&seconds);
                ss.str("");
                ss.clear();
                ss << std::put_time(local_time, "%Y-%m-%d %H:%M:%S");
                lb.timeSent = ss.str();



                int senderCount = status.senderaddresses_size();
                lb.curSenders.clear();

                for (int i=0; i < senderCount; i++) {
                    std::string addr = status.senderaddresses(i);
                    lb.curSenders.insert(addr);
                }


                int workerCount = status.workers_size();

                for (int i=0; i < workerCount; i++) {
                    auto worker = status.workers(i);
                    std::string workerName = worker.name();

                    // Either returns the entry at this key, or creates one if none exists
                    auto & stats = lb.clientStats[workerName];
                    stats.name          = workerName;
                    stats.fillPercent   = worker.fillpercent();
                    stats.controlSignal = worker.controlsignal();
                    stats.slotsAssigned = worker.slotsassigned();
                    stats.lastUpdated   = worker.lastupdated();
                    stats.updateTime = google::protobuf::util::TimeUtil::TimestampToMilliseconds(stats.lastUpdated);

                    // Convert msec to seconds
                    time_t seconds = stats.updateTime/1000;

                    // Create a struct to hold the local time
                    struct tm *local_time = localtime(&seconds);

                    ss.str("");  // Clear the content
                    ss.clear();  // Reset the error state

                    // The formatted date and time
                    ss << std::put_time(local_time, "%Y-%m-%d %H:%M:%S");
                    stats.updateTimeString = ss.str();
                }

                // put lb somewhere
                lbStats[lb.lbId] = lb;
            }

            return 0;
        }



        /////////////////////////////////
        // LbAdmin class
        /////////////////////////////////


        /**
         * Constructor.
         * @param cIp       grpc IP address of control plane (dotted decimal format).
         * @param cPort     grpc port of control plane.
         * @param name      name of LB being reserved.
         * @param token     administration token.
         * @param senders   vector of IP addresses allowed to send to LB.
         * @param until     seconds since epoch until which to reserve the LB.
         *
         */
        LbAdmin::LbAdmin (const std::string& cpIP, uint16_t cpPort,
                                      const std::string& token) :

                cpAddr(cpIP), cpPort(cpPort), adminToken(token) {

            _stub = createStub(cpIP, cpPort);
        }


        /**
         * Reserve a specified LB to use.
         * Any print statement in this method will mess up the
         * URI value written into the EJFAT_URI env variable.
         * This will happen if there's an error.
         *
         * @param name      name of LB being reserved.
         * @param senders   set of IP addresses allowed to send to LB.
         * @param until     seconds since epoch until which to reserve the LB.
         *
         * @return 0 if successful, 1 if error in grpc communication
         *           or until in already in the past.
         */
        int LbAdmin::ReserveLoadBalancer(const std::string& name,
                                         const std::set<std::string> &senders,
                                         int64_t until) {

            // Reserve-LB message we are sending to server
            ReserveLoadBalancerRequest request;

            request.set_name(name);

            // Set the time for this reservation to run out
            auto timestamp = new google::protobuf::Timestamp{};
            timestamp->set_seconds(until);
            timestamp->set_nanos(0);

            // store locally
            lb.until = *timestamp;
            lb.untilSeconds = until;

            // Give ownership of object to protobuf
            request.set_allocated_until(timestamp);

            // add sender IP addresses, but check if they are valid
            std::set<std::string> validSenders;

            for (auto s : senders) {
                try {
                    boost::asio::ip::make_address(s);
                }
                catch (const boost::system::system_error& e) {
                    std::cout << "skip bad ip addr, " << s << std::endl;
                    continue;
                }
                request.add_senderaddresses(s);
                validSenders.insert(s);
            }

            // Container for the response we expect from server
            ReserveLoadBalancerReply reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            // Set bearer token in header.
            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            // The actual RPC
            Status status = _stub->ReserveLoadBalancer(&context, request, &reply);

            // Act upon its status
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            // things returned from CP
            lb.name = name;
            lb.senders = validSenders;

            lb.instanceToken   = reply.token();
            lb.lbId            = reply.lbid();
            lb.syncIpAddress   = reply.syncipaddress();
            lb.syncUdpPort     = reply.syncudpport();
            lb.dataIpv4Address = reply.dataipv4address();
            lb.dataIpv6Address = reply.dataipv6address();
            lb.fpgaLbId        = reply.fpgalbid();

            isReserved = true;

            // Create URIs to use

            std::stringstream ss;

            if (!reply.dataipv6address().empty()) {
                ss << "ejfat://" << reply.token() << "@" << cpAddr << ":" << cpPort;
                ss << "/lb/" << reply.lbid();
                ss << "?data=" << reply.dataipv6address() << ":19522";
                ss << "&sync=" << reply.syncipaddress() << ":" << reply.syncudpport();
                uri6 = ss.str();
            }


            if (!reply.dataipv4address().empty()) {
                ss.str("");  // Clear the content
                ss.clear();  // Reset the error state
                ss << "ejfat://" << reply.token() << "@" << cpAddr << ":" << cpPort;
                ss << "/lb/" << reply.lbid();
                ss << "?data=" << reply.dataipv4address() << ":19522";
                ss << "&sync=" << reply.syncipaddress() << ":" << reply.syncudpport();
                uri4 = ss.str();
            }

            return 0;
        }



        /**
         * Add to the list of approved senders.
         * @param senders senders to add.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbAdmin::AddSenders(const std::set<std::string> &senders) {

            // Add senders message we are sending to server
            AddSendersRequest request;

            request.set_lbid(lb.lbId);

            // Add sender IP addresses, check validity
            int senderCount = 0;
            for (auto s : senders) {
                try {
                    boost::asio::ip::make_address(s);
                }
                catch (const boost::system::system_error& e) {
                    std::cout << "skip bad ip addr, " << s << std::endl;
                    continue;
                }

                // Check if already in sender set
                auto result = lb.curSenders.insert(s);

                // True if insertion took place
                if (result.second) {
                    request.add_senderaddresses(s);
                    senderCount++;
                }
            }

            if (senderCount < 1) {
                // These senders were all already known
                return 0;
            }

            AddSendersReply reply;

            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            Status status = _stub->AddSenders(&context, request, &reply);
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            // Nothing returned from CP

            return 0;
        }


        /**
         * Remove from the list of approved senders.
         * @param senders senders to remove.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbAdmin::RemoveSenders(const std::set<std::string> &senders) {

            // Remove senders message we are sending to server
            RemoveSendersRequest request;

            request.set_lbid(lb.lbId);

            // Remove sender IP addresses, check validity
            int senderCount = 0;
            for (auto s : senders) {
                try {
                    boost::asio::ip::make_address(s);
                }
                catch (const boost::system::system_error& e) {
                    std::cout << "skip bad ip addr, " << s << std::endl;
                    continue;
                }

                // Attempt to remove a sender
                int numErased = lb.curSenders.erase(s);

                if (numErased > 0) {
                    request.add_senderaddresses(s);
                    senderCount++;
                }
            }

            if (senderCount < 1) {
                // These senders were all already removed
                return 0;
            }

            RemoveSendersReply reply;

            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            Status status = _stub->RemoveSenders(&context, request, &reply);
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            // Nothing returned from CP

            return 0;
        }



        /**
         * Free the LB from a single reserved slot.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbAdmin::FreeLoadBalancer() {

            // Free-LB message we are sending to server
            FreeLoadBalancerRequest request;
            request.set_lbid(lb.lbId);

            FreeLoadBalancerReply reply;

            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            Status status = _stub->FreeLoadBalancer(&context, request, &reply);
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            isReserved = false;
            return 0;
        }




        /**
         * Get LB status.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbAdmin::LoadBalancerStatus() {
            // LB-request-for-status message we are sending to server
            LoadBalancerStatusRequest request;

            request.set_lbid(lb.lbId);

            LoadBalancerStatusReply reply;

            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            Status status = _stub->LoadBalancerStatus(&context, request, &reply);

            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            //----------------------------
            // Things returned from CP

            std::stringstream ss;

            lb.curEpoch = reply.currentepoch();
            lb.curPredictedEventNum = reply.currentpredictedeventnumber();

            lb.expiresAt = reply.expiresat();
            lb.expireAtMilliSeconds = google::protobuf::util::TimeUtil::TimestampToMilliseconds(lb.expiresAt);

            // Convert msec to seconds
            time_t seconds = lb.expireAtMilliSeconds / 1000;

            // Create a struct to hold the local time
            struct tm *local_time = localtime(&seconds);

            ss.str("");  // Clear the content
            ss.clear();  // Reset the error state

            // The formatted date and time
            ss << std::put_time(local_time, "%Y-%m-%d %H:%M:%S");
            lb.expireAtString = ss.str();

            // How many senders?
            int senderCount = reply.senderaddresses_size();
            lb.curSenders.clear();
            for (int j = 0; j < senderCount; j++) {
                lb.curSenders.insert(reply.senderaddresses(j));
            }

            // How many clients on this LB?
            int clientCount = reply.workers_size();
            lb.clientStats.clear();

            for (int j = 0; j < clientCount; j++) {
                std::string name = reply.workers(j).name();

                // Either returns the entry at this key, or creates one if none exists
                auto & stats = lb.clientStats[name];
                stats.fillPercent   = reply.workers(j).fillpercent();
                stats.controlSignal = reply.workers(j).controlsignal();
                stats.slotsAssigned = reply.workers(j).slotsassigned();
                stats.lastUpdated   = reply.workers(j).lastupdated();
                stats.updateTime = google::protobuf::util::TimeUtil::TimestampToMilliseconds(stats.lastUpdated);

                time_t seconds = stats.updateTime / 1000;
                struct tm *local_time = localtime(&seconds);

                ss.str("");  // Clear the content
                ss.clear();  // Reset the error state

                ss << std::put_time(local_time, "%Y-%m-%d %H:%M:%S");
                stats.updateTimeString = ss.str();
            }

            return 0;
        }



        /**
        * Function to determine if a string is an IPv4 address.
        * @param address string containing address to examine.
        */
        static bool is_ipv4(const std::string& str) {
            std::regex ipv4_regex("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");
            return std::regex_match(str, ipv4_regex);
        }

        /**
         * Function to determine if a string is an IPv6 address.
         * @param address string containing address to examine.
         */
        static bool is_ipv6(const std::string& str) {
            std::regex ipv6_regex("^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$");
            return std::regex_match(str, ipv6_regex);
        }


        /**
         * Function to take a host name and turn it into IP addresses, IPv4 and IPv6.
         *
         * @param host_name name of host to examine.
         * @param ipv4 IP version 4 dot-decimal form of host_name if available.
         * @param ipv6 IP version 6 dot-decimal form of host_name if available.
         * @return true if successfully ran function, else false if no address info available.
         */
        static bool resolve_host(const std::string& host_name, std::string& ipv4, std::string& ipv6) {
            struct addrinfo hints, *result;
            std::memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_UNSPEC; // Allow IPv4 or IPv6
            hints.ai_socktype = SOCK_STREAM;

            int status = getaddrinfo(host_name.c_str(), nullptr, &hints, &result);
            if (status != 0) {
                std::cerr << "resolveHost: getaddrinfo error: " << gai_strerror(status) << std::endl;
                return false;
            }

            void* addr;
            char ipstr[INET6_ADDRSTRLEN];
            for (struct addrinfo* p = result; p != nullptr; p = p->ai_next) {
                if (p->ai_family == AF_INET) { // IPv4
                    struct sockaddr_in* ipv4 = reinterpret_cast<struct sockaddr_in*>(p->ai_addr);
                    addr = &(ipv4->sin_addr);
                } else { // IPv6
                    struct sockaddr_in6* ipv6 = reinterpret_cast<struct sockaddr_in6*>(p->ai_addr);
                    addr = &(ipv6->sin6_addr);
                }
                // Convert the IP to a string and return it
                inet_ntop(p->ai_family, addr, ipstr, sizeof(ipstr));

                if (is_ipv4(ipstr)) {
                    ipv4 = ipstr;
                    //std::cerr << "got IP v4 addr: " << ipstr << std::endl;
                }

                if (is_ipv6(ipstr)) {
                    ipv6 = ipstr;
                    //std::cerr << "got IP v6 addr: " << ipstr << std::endl;
                }
            }

            freeaddrinfo(result); // Free memory
            return true;
        }



        /**
         * STATIC method to reserve a specified LB to use.
         * The resultant URI is returned.
         * Any print statement in this method will mess up the execution of lbreserve.
         *
         * @param cpIP          control plane IP address for grpc communication.
         * @param cpPort        control plane TCP port for grpc communication.
         * @param lbName        name to assign this LB.
         * @param adminToken    token used to interact with LB.
         * @param senders       set of IP addresses allowed to send to LB.
         * @param untilSeconds  time (seconds past epoch) at which reservation ends.
         * @param useIPv6       use IP version 6 destination address when constructing
         *                      URI containing info for sending data.
         *
         * @return resulting URI starting with "ejfat",
         *         else error string starting with "error".
         */
        std::string LbAdmin::ReserveLoadBalancer(const std::string& cpIP, uint16_t cpPort,
                                                 const std::string& lbName,
                                                 const std::string& adminToken,
                                                 const std::set<std::string> &senders,
                                                 int64_t untilSeconds, bool useIPv6) {

            auto _stub = createStub(cpIP, cpPort);

            // Reserve-LB message we are sending to server
            ReserveLoadBalancerRequest request;

            request.set_name(lbName);

            // Set the time for this reservation to run out
            auto timestamp = new google::protobuf::Timestamp{};
            timestamp->set_seconds(untilSeconds);
            timestamp->set_nanos(0);
            // Give ownership of object to protobuf
            request.set_allocated_until(timestamp);

            // add sender IP addresses, but check they are valid
            for (auto s : senders) {
                try {
                    boost::asio::ip::make_address(s);
                }
                catch (const boost::system::system_error& e) {
                    std::cout << "skip bad ip addr, " << s << std::endl;
                    continue;
                }
                request.add_senderaddresses(s);
//std::cerr << "Include sender " << s << " when requesting LB" << std::endl;
            }

            // Container for the response we expect from server
            ReserveLoadBalancerReply reply;

            // Set bearer token in header.
            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            // The actual RPC
            Status status = _stub->ReserveLoadBalancer(&context, request, &reply);

            // cpIP may have been specified as a host name and not in dot-decimal form.
            // Convert it now if necessary since we're going to need it in creating
            // our ejfat URI.
            std::string ipAddr=cpIP;
            if (!(is_ipv4(cpIP) || is_ipv6(cpIP))) {
                std::string ipV4, ipV6;
                // convert to dot decimal
                resolve_host(cpIP, ipV4, ipV6);

                if (useIPv6 && !ipV6.empty()) {
//std::cerr << "Converted " << cpIP << " into v6 " << ipV6 << std::endl;
                    ipAddr = ipV6;
                }
                else if (!ipV4.empty()) {
//std::cerr << "Converted " << cpIP << " into v4 " << ipV4 << std::endl;
                    ipAddr = ipV4;
                }
            }

            // Act upon its status
            char url[256];

            if (!status.ok()) {
                //std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                sprintf(url, "error = %s", status.error_message().c_str());
            }
            else {
                // Things returned from CP used to create ejfat URI
                if (useIPv6) {
                    sprintf(url, "ejfat://%s@%s:%hu/lb/%s?data=%s:%d&sync=%s:%d",
                            reply.token().c_str(),
                            ipAddr.c_str(), cpPort, reply.lbid().c_str(),
                            reply.dataipv6address().c_str(), 19522,
                            reply.syncipaddress().c_str(), reply.syncudpport());
                }
                else {
                    sprintf(url, "ejfat://%s@%s:%hu/lb/%s?data=%s:%d&sync=%s:%d",
                            reply.token().c_str(),
                            ipAddr.c_str(), cpPort, reply.lbid().c_str(),
                            reply.dataipv4address().c_str(), 19522,
                            reply.syncipaddress().c_str(), reply.syncudpport());

                }
            }

            return std::string(url);
        }



        /**
         * STATIC method to add to the list of approved senders.
         *
         * @param cpIP          control plane IP address for grpc communication.
         * @param cpPort        control plane TCP port for grpc communication.
         * @param lbId          id of LB to be freed.
         * @param adminToken    token used to interact with LB.
         * @param senders       senders to add.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbAdmin::AddSenders(const std::string& cpIP, uint16_t cpPort,
                                const std::string& lbId,
                                const std::string& adminToken,
                                const std::set<std::string> &senders) {

            auto _stub = createStub(cpIP, cpPort);

            // Add senders message we are sending to server
            AddSendersRequest request;

            request.set_lbid(lbId);

            // Add sender IP addresses, check validity
            for (auto s : senders) {
                try {
                    boost::asio::ip::make_address(s);
                }
                catch (const boost::system::system_error& e) {
                    std::cout << "skip bad ip addr, " << s << std::endl;
                    continue;
                }

                request.add_senderaddresses(s);
            }

            AddSendersReply reply;

            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            Status status = _stub->AddSenders(&context, request, &reply);
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            // Nothing returned from CP

            return 0;
        }



        /**
         * STATIC method to remove from the list of approved senders.
         *
         * @param cpIP          control plane IP address for grpc communication.
         * @param cpPort        control plane TCP port for grpc communication.
         * @param lbId          id of LB to be freed.
         * @param adminToken    token used to interact with LB.
         * @param senders       senders to remove.
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbAdmin::RemoveSenders(const std::string& cpIP, uint16_t cpPort,
                                const std::string& lbId,
                                const std::string& adminToken,
                                const std::set<std::string> &senders) {

            auto _stub = createStub(cpIP, cpPort);

            // Remove senders message we are sending to server
            RemoveSendersRequest request;

            request.set_lbid(lbId);

            // Remove sender IP addresses, check validity
            for (auto s : senders) {
                try {
                    boost::asio::ip::make_address(s);
                }
                catch (const boost::system::system_error& e) {
                    std::cout << "skip bad ip addr, " << s << std::endl;
                    continue;
                }

                request.add_senderaddresses(s);
            }

            RemoveSendersReply reply;

            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            Status status = _stub->RemoveSenders(&context, request, &reply);
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            // Nothing returned from CP

            return 0;
        }



        /**
         * STATIC method to free the LB from a single reserved slot.
         *
         * @param cpIP          control plane IP address for grpc communication.
         * @param cpPort        control plane TCP port for grpc communication.
         * @param lbId          id of LB to be freed.
         * @param adminToken    token used to interact with LB.
         *
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbAdmin::FreeLoadBalancer(const std::string& cpIP, uint16_t cpPort,
                                      const std::string& lbId,
                                      const std::string& adminToken) {

            auto _stub = createStub(cpIP, cpPort);

            // Free-LB message we are sending to server
            FreeLoadBalancerRequest request;
            request.set_lbid(lbId);

            // Container for the response we expect from server
            FreeLoadBalancerReply reply;

            // Set bearer token in header.
            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            // The actual RPC
            Status status = _stub->FreeLoadBalancer(&context, request, &reply);

            // Act upon its status
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }
            return 0;
        }



        /**
         * STATIC method to get LB status info.
         *
         * @param cpIP          control plane IP address for grpc communication.
         * @param cpPort        control plane TCP port for grpc communication.
         * @param lbId          id of LB to be freed.
         * @param adminToken    token used to interact with LB.
         * @param clientStats   ref to map in which to store LB client stats.
         *
         * @return 0 if successful, 1 if error in grpc communication
         */
        int LbAdmin::LoadBalancerStatus(const std::string& cpIP, uint16_t cpPort,
                                        const std::string& lbId,
                                        const std::string& adminToken,
                                        std::unordered_map<std::string, LbClientStatus>& clientStats) {

            auto _stub = createStub(cpIP, cpPort);

            // LB-request-for-status message we are sending to server
            LoadBalancerStatusRequest request;

            request.set_lbid(lbId);

            // Container for the response we expect from server
            LoadBalancerStatusReply reply;

            // Set bearer token in header.
            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            // The actual RPC
            Status status = _stub->LoadBalancerStatus(&context, request, &reply);

            // Act upon its status
            if (!status.ok()) {
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return 1;
            }

            // Things returned from CP

            std::stringstream ss;

            // How many clients on this LB?
            int clientCount = reply.workers_size();

            for (int j = 0; j < clientCount; j++) {
                std::string name = reply.workers(j).name();

                // Either returns the entry at this key, or creates one if none exists
                auto & stats = clientStats[name];
                stats.fillPercent   = reply.workers(j).fillpercent();
                stats.controlSignal = reply.workers(j).controlsignal();
                stats.slotsAssigned = reply.workers(j).slotsassigned();
                stats.lastUpdated   = reply.workers(j).lastupdated();
                stats.updateTime = google::protobuf::util::TimeUtil::TimestampToMilliseconds(stats.lastUpdated) / 1000;

                // Convert msec to sec
                time_t seconds = stats.updateTime / 1000;

                // Create a struct to hold the local time
                struct tm *local_time = localtime(&seconds);

                ss.str("");  // Clear the content
                ss.clear();  // Reset the error state

                // The formatted date and time
                ss << std::put_time(local_time, "%Y-%m-%d %H:%M:%S");
                stats.updateTimeString = ss.str();
            }

            return 0;
        }



        /**
         * STATIC method to get LB connection info, but without the token.
         *
         * @param cpIP          control plane IP address for grpc communication.
         * @param cpPort        control plane TCP port for grpc communication.
         * @param lbId          id of LB to be freed.
         * @param adminToken    token used to interact with LB.
         * @param useIPv6       use IP version 6 destination address when constructing
         *                      URI containing info for sending data.
         *
         * @return resulting URI starting with "ejfat",
         *         else error string starting with "error".
         */
        std::string LbAdmin::GetLbUri(const std::string& cpIP, uint16_t cpPort,
                                      const std::string& lbId,
                                      const std::string& adminToken,
                                      bool useIPv6) {

            auto _stub = createStub(cpIP, cpPort);

            // LB-request-for-connection info message we are sending to server
            GetLoadBalancerRequest request;

            request.set_lbid(lbId);

            // Container for the response we expect from server
            ReserveLoadBalancerReply reply;

            // Set bearer token in header.
            ClientContext context;
            context.AddMetadata("authorization", "Bearer " + adminToken);

            // The actual RPC
            Status status = _stub->GetLoadBalancer(&context, request, &reply);

            // cpIP may have been specified as a host name and not in dot-decimal form.
            // Convert it now if necessary since we're going to need it in creating
            // our ejfat URI.
            std::string ipAddr=cpIP;
            if (!(is_ipv4(cpIP) || is_ipv6(cpIP))) {
                std::string ipV4, ipV6;
                // convert to dot decimal
                resolve_host(cpIP, ipV4, ipV6);

                if (useIPv6 && !ipV6.empty()) {
//std::cerr << "Converted " << cpIP << " into v6 " << ipV6 << std::endl;
                    ipAddr = ipV6;
                }
                else if (!ipV4.empty()) {
//std::cerr << "Converted " << cpIP << " into v4 " << ipV4 << std::endl;
                    ipAddr = ipV4;
                }
            }

            // Act upon its status
            char url[256];

            if (!status.ok()) {
                //std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                sprintf(url, "error = %s", status.error_message().c_str());
            }
            else {
                // Things returned from CP used to create ejfat URI
                if (useIPv6) {
                    sprintf(url, "ejfat://%s:%hu/lb/%s?data=%s:%d&sync=%s:%d",
                            ipAddr.c_str(), cpPort, reply.lbid().c_str(),
                            reply.dataipv6address().c_str(), 19522,
                            reply.syncipaddress().c_str(), reply.syncudpport());
                }
                else {
                    sprintf(url, "ejfat://%s:%hu/lb/%s?data=%s:%d&sync=%s:%d",
                            ipAddr.c_str(), cpPort, reply.lbid().c_str(),
                            reply.dataipv4address().c_str(), 19522,
                            reply.syncipaddress().c_str(), reply.syncudpport());

                }
            }

            return url;
        }



        // Getters
        const std::string & LbAdmin::getAdminToken() const {return adminToken;}
        const std::string & LbAdmin::getCpAddr()     const {return cpAddr;}
        uint16_t            LbAdmin::getCpPort()     const {return cpPort;}
        const std::string & LbAdmin::getUri4()       const {return uri4;}
        const std::string & LbAdmin::getUri6()       const {return uri6;}
        bool LbAdmin::reserved() const {return isReserved && !lb.reservationElapsed();}



