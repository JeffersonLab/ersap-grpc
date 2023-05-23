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
            authToken       = req->authtoken();
        	name            = req->name();

            cpus            = req->cpus();
        	ramBytes        = req->rambytes();
        	bufCount        = req->bufcount();
        	bufSizeBytes    = req->bufsizebytes();
            setPointPercent = req->setpointpercent();

            targetIP        = req->dplanetargetip();
            targetPort      = req->dplanetargetport();
            targetPortRange = req->dplanetargetportrange();
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

            sessionToken = state->sessiontoken();
            fillPercent  = state->fillpercent();
            pidError     = state->piderror();
            isReady      = state->isready();
        }


        /** Print out backend status. */
        void BackEnd::printBackendState() const {
        	std::cout << "State of "        << name
                      << " @ t = "          << time
                      << " : buf cnt = "    << bufCount
                      << ", buf size = "    << bufSizeBytes
                      << ", set pt = "      << setPointPercent
                      << ", fill % = "      << fillPercent
                      << ", pid error % = " << pidError
                      << ", ready = "       << isReady
                      << std::endl;
       }


        /** Get the backend's authentication token.
         *  @return backend's authentication token. */
        const std::string & BackEnd::getAuthToken() const {return authToken;}

        /** Get the backend's session token.
         *  @return backend's session token. */
        const std::string & BackEnd::getSessionToken() const {return sessionToken;}

        /** Get the backend's name.
         *  @return backend's name. */
        const std::string & BackEnd::getName() const {return name;}


        /** Get the timestamp of the latest backend's sent data.
         *  @return timestamp of the latest backend's sent data. */
        google::protobuf::Timestamp BackEnd::getTimestamp() const {return timestamp;}

        /** Get the timestamp of the latest backend's sent data.
         *  @return timestamp of the latest backend's sent data. */
        int64_t BackEnd::getTime() const {return time;}

        /** Get the locally set timestamp of the arrival of the latest backend's sent data.
         *  @return locally set timestamp of the arrival of the latest backend's sent data. */
        int64_t BackEnd::getLocalTime() const {return localTime;}


        /** Get total number of cores on backend.
         *  @return backend's number of cores. */
        uint32_t BackEnd::getCpus() const {return cpus;}

        /** Get total RAM in backend.
         *  @return backend's total RAM. */
        uint32_t BackEnd::getRamBytes() const {return ramBytes;}

        /** Get the backend's total number of fifo entries (buffers).
         *  @return backend's total number of fifo entries. */
        uint32_t BackEnd::getBufCount() const {return bufCount;}

        /** Get the byte size of each backend fifo entry (buffer).
         *  @return byte size of each backend fifo entry. */
        uint32_t BackEnd::getBufSizeBytes() const {return bufSizeBytes;}

        
        /** Get the fill percentage of the backend fifo (0-1).
         *  @return fill percentage of the backend fifo. */
        float BackEnd::getFillPercent() const {return fillPercent;}
        
        /** Get the set point of the backend in % fifo filled (0-1).
         *  @return set point of the backend. */
        float BackEnd::getSetPointPercent() const {return setPointPercent;}
        
        /** Get the PID loop's error term in % fifo (0 - +/- 0.5).
         *  @return PID loop's error term. */
        float BackEnd::getPidError() const {return pidError;}


        /** Get the backend's receiving IP address (dot-decimal).
         *  @return backend's receiving IP address. */
        const std::string & BackEnd::getTargetIP() const {return targetIP;}

        /** Get the backend's receiving UDP port.
         *  @return backend's receiving UDP port. */
        uint32_t BackEnd::getTargetPort() const {return targetPort;}

        /** Get the backend's receiving UDP port.
         *  @return backend's receiving UDP port. */
        uint32_t BackEnd::getTargetPortRange() const {return targetPortRange;}


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
		// LoadBalancerServiceImpl class
		/////////////////////////////////

//        Status LoadBalancerServiceImpl::SendStateAsync(ServerContext* context,
//                                                        const SendStateRequest* state,
//                                                        ServerAsyncResponseWriter<SendStateReply> *responder,
//                                                        CompletionQueue *cq1, CompletionQueue *cq2,
//                                                        void  *ptr) {
//
//            return Status::OK;
//        }


        /**
         * Send the state of the backend to server / control-plane.
         * @param contex   unused here
         * @param request  unused here
         * @param reply    reply to send to client
         */
        Status LoadBalancerServiceImpl::SendState (ServerContext* context,
                                                    const SendStateRequest* state,
                                                    SendStateReply* reply) {

            // Protect map since there's multithreaded access
            const std::lock_guard<std::mutex> lock(map_mutex);

            // Search on the token
            auto backend = data.find(state->sessiontoken());

            // If not registered
            if (backend == data.end()) {
std::cout << "Receives state of UNREGISTERED session token " << state->sessiontoken() << ", ignore" << std::endl;
                return Status::OK;
            }

//std::cout << "Received state of session token " << state->sessiontoken() << std::endl;

            backend->second.update(state);

            return Status::OK;
        }


        /**
         * Register a backend with the control plane.
         * @param contex   unused here
         * @param request  unused here
         * @param reply    reply to send to client
         */
		Status LoadBalancerServiceImpl::Register (ServerContext* context,
		                                           const RegisterRequest* request,
		                                           RegisterReply* reply) {

            const std::lock_guard<std::mutex> lock(map_mutex);

std::cout << "Currently NO authentication needed for " << request->name() << std::endl;

            // Generate a unique session token
            uint64_t val = backendToken.fetch_add(1, std::memory_order_relaxed);
            std::string sessionToken = std::to_string(val);

            // Save backend info and place in a map
            BackEnd be(request);
			data.insert(std::make_pair(sessionToken, be));

std::cout << "Registered client\"" << request->name() << "\" (as token " << val << ")" << std::endl;

	        reply->set_sessiontoken(sessionToken);
	        return Status::OK;
	    }
	     		
        
        /**
         * Unregister a backend from the control plane.
         * @param contex   unused here
         * @param request  unused here
         * @param reply    reply to send to client
         */
		Status LoadBalancerServiceImpl::DeRegister (ServerContext* context,
		                                             const DeregisterRequest* request,
                                                     DeregisterReply* reply) {

		    const std::lock_guard<std::mutex> lock(map_mutex);
            data.erase(request->sessiontoken());
std::cout << "De-registered client token " << request->sessiontoken() << std::endl;
            return Status::OK;
	    }
	     		
             
        /**
         * Get a COPY of the map containing all BackEnd objects.
         * Copying the map while mutex protected and then using it will keep things threadsafe.
         * This method will need to be called each time updated data is needed.
         * @return COPY of map containing all BackEnd objects.
         */       
        std::shared_ptr<std::unordered_map<std::string, BackEnd>> LoadBalancerServiceImpl::getBackEnds() {
            const std::lock_guard<std::mutex> lock(map_mutex);
            return std::make_shared<std::unordered_map<std::string, BackEnd>>(data);
        }


//helloworld::Greeter::AsyncService service;
//ServerBuilder builder;
//builder.AddListeningPort("0.0.0.0:50051", InsecureServerCredentials());
//builder.RegisterService(&service);
//auto cq = builder.AddCompletionQueue();
//auto server = builder.BuildAndStart();


        /**
         * Blocking call to run local server.
         * @param port port to run server on.
         */
        void LoadBalancerServiceImpl::runServer(uint16_t port, LoadBalancerServiceImpl *service) {
		    std::cout << "in runServer method on port " << port << std::endl;
		    std::string server_address("0.0.0.0:" + std::to_string(port));
		    //std::string server_address2("0.0.0.0:50051");
		    //LoadBalancerServiceImpl service;
            std::cout << "Server listening on " << server_address << std::endl;

		    grpc::EnableDefaultHealthCheckService(true);
		    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
		    ServerBuilder builder;
		    // Listen on the given address without any authentication mechanism.
		    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
		    // Register "service" as the instance through which we'll communicate with
		    // clients. In this case it corresponds to an *synchronous* service.
		    builder.RegisterService(service);
//            cq = builder.AddCompletionQueue();
		    // Finally assemble the server.
		    std::unique_ptr<Server> server(builder.BuildAndStart());
		    std::cout << "Server listening on " << server_address << std::endl;

		    // Wait for the server to shutdown. Note that some other thread must be
		    // responsible for shutting down the server for this call to ever return.
		    server->Wait();
        }
        
        
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
         * @param bufferSize   byte size of each buffer (fifo entry) in this backend.
         * @param bufferCount  number of buffers in fifo.
         * @param setPoint     PID loop set point (% of fifo).
         * 
         */
        LbControlPlaneClient::LbControlPlaneClient(
                             const std::string& cIP, uint16_t cPort,
                             const std::string& bIP, uint16_t bPort,
                             PortRange bPortRange, const std::string& cliName,
                             uint32_t bufferSize, uint32_t bufferCount, float setPoint) :
                             cpAddr(cIP), cpPort(cPort), beAddr(bIP), bePort(bPort),
                             beRange(bPortRange), name(cliName),
                             bufSizeBytes(bufferSize), bufCount(bufferCount), setPointPercent(setPoint) {
                
		    cpTarget = cIP + ":" + std::to_string(cPort);
		    stub_ = LoadBalancer::NewStub(grpc::CreateChannel(cpTarget, grpc::InsecureChannelCredentials()));
        }  
        
  
	    /**
         * Update internal state of this object (eventually sent to control plane).
         * @param fill     % of fifo filled
         * @param pdErr    pid error (units of % fifo filled)
         */
        void LbControlPlaneClient::update(float fill, float pidErr) {
        	fillPercent = fill;
        	pidError = pidErr;
        	isReady = true;
        	if (fill >= 0.95) {
        	    isReady = false;
        	}
        }



        /**
		 * Register this backend with the control plane.
		 * @return 0 if successful, 1 if error in grpc communication
		 */
    	int32_t LbControlPlaneClient::Register() {
		    // Registration message we are sending to server
		    RegisterRequest request;

            request.set_authtoken(name);
            request.set_name(name);

            // Get # of cores (0 if unknown) (since C++ 11)
            unsigned int nthreads = std::thread::hardware_concurrency();
            request.set_cpus(nthreads);

            // Find the amount of RAM
            long page_size = sysconf(_SC_PAGE_SIZE);
            long pages = 0;
            uint64_t mem;

#ifdef __linux__

            // NOT POSIX
            pages = sysconf(_SC_PHYS_PAGES);

#elif __APPLE__

            size_t len = sizeof(mem);
            sysctlbyname("hw.memsize", &mem, &len, NULL, 0);

#endif

            mem = pages * page_size;
            request.set_rambytes(mem);

            request.set_bufsizebytes(bufSizeBytes);
		    request.set_bufcount(bufCount);
		    request.set_setpointpercent(setPointPercent);

		    // Network info for this client
            request.set_dplanetargetip(beAddr);
            request.set_dplanetargetport(bePort);
            request.set_dplanetargetportrange(beRange);

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

		    sessionToken = reply.sessiontoken();
		    return 0;
    	}
    	
 
        /**
		 * Unregister this backend with the control plane.
		 * @return 0 if successful, 1 if error in grpc communication
		 */
    	int LbControlPlaneClient::Deregister() const {

    	    // Deregistration message we are sending to server
		    DeregisterRequest request;
		    request.set_sessiontoken(sessionToken);

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

            request.set_sessiontoken(sessionToken);

            // Set the time
            struct timespec t1;
            clock_gettime(CLOCK_REALTIME, &t1);
            auto timestamp = new google::protobuf::Timestamp{};
            timestamp->set_seconds(t1.tv_sec);
            timestamp->set_nanos(t1.tv_nsec);
            // Give ownership of object to protobuf
            request.set_allocated_timestamp(timestamp);

            request.set_fillpercent(fillPercent);
            request.set_piderror(pidError);
            request.set_isready(isReady);

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
        const std::string & LbControlPlaneClient::getCpAddr()    const   {return cpAddr;}
        const std::string & LbControlPlaneClient::getDataAddr()  const   {return beAddr;}
        const std::string & LbControlPlaneClient::getName()      const   {return name;}

        uint16_t   LbControlPlaneClient::getCpPort()             const   {return cpPort;}
        uint16_t   LbControlPlaneClient::getDataPort()           const   {return bePort;}
		uint32_t   LbControlPlaneClient::getBufCount()           const   {return bufCount;}
		uint32_t   LbControlPlaneClient::getBufSizeBytes()       const   {return bufSizeBytes;}

		PortRange  LbControlPlaneClient::getDataPortRange()      const   {return beRange;}

		float      LbControlPlaneClient::getSetPointPercent()    const   {return setPointPercent;}
		float      LbControlPlaneClient::getFillPercent()        const   {return fillPercent;}
        float      LbControlPlaneClient::getPidError()           const   {return pidError;}

        bool       LbControlPlaneClient::getIsReady()            const   {return isReady;}

    
  

