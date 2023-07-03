//
// Copyright 2022, Jefferson Science Associates, LLC.
// Subject to the terms in the LICENSE file found in the top-level directory.
//
// EPSCI Group
// Thomas Jefferson National Accelerator Facility
// 12000, Jefferson Ave, Newport News, VA 23606
// (757)-269-7100

/**
 * @file
 * Simulate a load balancer's control plane by receiving gRPC messages from an ERSAP (simulated) backend --
 * packetBlasteeEtFifoClient.c, control_plane_tester.c, cp_tester.cc programs.
 */

#include <memory>
#include <string>

#include <cstdlib>
#include <iostream>
#include <ctime>
#include <thread>
#include <cmath>
#include <chrono>
#include <atomic>
#include <algorithm>
#include <cstring>
#include <cerrno>
#include <cinttypes>
#include <getopt.h>
#include <random>
#include <map>
#include <unordered_map>

#ifdef __linux__
    #ifndef _GNU_SOURCE
        #define _GNU_SOURCE
    #endif

    #include <sched.h>
    #include <pthread.h>
#endif


#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#include "examples/protos/loadbalancer.grpc.pb.h"
#else
#include "loadbalancer.grpc.pb.h"
#endif

#include "lb_cplane.h"
#include "ersap_grpc_assemble.hpp"



using namespace std;

//-----------------------------------------------------------------------
// Be sure to print to stderr as this program pipes data to stdout!!!
//-----------------------------------------------------------------------


#define INPUT_LENGTH_MAX 256


/**
 * Print out help.
 * @param programName name to use for this program.
 */
static void printHelp(char *programName) {
    fprintf(stderr,
            "\nusage: %s\n%s\n%s\n%s\n%s\n\n",
            programName,
            "        [-h] [-v] [-ip6]",
            "        [-p <grpc server port>]",
            "        [-sport <sync msg port>]",
            "        [-cores <comma-separated list of cores to run on>]");

    fprintf(stderr, "        This is a gRPC server getting requests/data from an ERSAP reasembly backend's gRPC client.\n");
    fprintf(stderr, "        It also receives sync msgs from data senders as to the latest event # sent.\n");
}


/**
 * Parse all command line options.
 *
 * @param argc          arg count from main().
 * @param argv          arg list from main().
 * @param cores         array of core ids on which to run assembly thread.
 * @param port          filled with port of gRPC server).
 * @param sport         filled with port of control plane's input for sync msgs).
 * @param debug         filled with debug flag.
 */
static void parseArgs(int argc, char **argv,
                      int *cores, uint16_t* port, uint16_t* sport,
                      bool *debug, bool *useIPv6) {

    int c, i_tmp;
    bool help = false;

    /* 4 multiple character command-line options */
    static struct option long_options[] =
            { {"cores",  1, NULL, 1},
              {"sport",  1, NULL, 2},
              {"ip6",      0, nullptr, 3},
               {0,       0, 0,    0}
            };


    while ((c = getopt_long_only(argc, argv, "vhp:b:a:r:f:", long_options, 0)) != EOF) {

        if (c == -1)
            break;

        switch (c) {

            case 'p':
                // PORT
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 1023 && i_tmp < 65535) {
                    *port = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -p, 1023 < port < 65536\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 2:
                // sync msg PORT
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 1023 && i_tmp < 65535) {
                    *sport = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -sport, 1023 < port < 65536\n");
                    exit(-1);
                }
                break;

            case 3:
                // use IP version 6
                fprintf(stderr, "SETTING TO IP version 6\n");
                *useIPv6 = true;
                break;

            case 1:
                // Cores to run on
                if (strlen(optarg) < 1) {
                    fprintf(stderr, "Invalid argument to -cores, need comma-separated list of core ids\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }


                {
                    // split into ints
                    std::string s = optarg;
                    std::string delimiter = ",";

                    size_t pos = 0;
                    std::string token;
                    char *endptr;
                    int index = 0;
                    bool oneMore = true;

                    while ((pos = s.find(delimiter)) != std::string::npos) {
                        //fprintf(stderr, "pos = %llu\n", pos);
                        token = s.substr(0, pos);
                        errno = 0;
                        cores[index] = (int) strtol(token.c_str(), &endptr, 0);

                        if ((token.c_str() - endptr) == 0) {
                            //fprintf(stderr, "two commas next to eachother\n");
                            oneMore = false;
                            break;
                        }
                        index++;
                        //std::cout << token << std::endl;
                        s.erase(0, pos + delimiter.length());
                        if (s.length() == 0) {
                            //fprintf(stderr, "break on zero len string\n");
                            oneMore = false;
                            break;
                        }
                    }

                    if (oneMore) {
                        errno = 0;
                        cores[index] = (int) strtol(s.c_str(), nullptr, 0);
                        if (errno == EINVAL || errno == ERANGE) {
                            fprintf(stderr, "Invalid argument to -cores, need comma-separated list of core ids\n\n");
                            printHelp(argv[0]);
                            exit(-1);
                        }
                        index++;
                        //std::cout << s << std::endl;
                    }
                }
                break;

            case 'v':
                // VERBOSE
                *debug = true;
                break;

            case 'h':
                help = true;
                break;

            default:
                printHelp(argv[0]);
                exit(2);
        }

    }

    if (help) {
        printHelp(argv[0]);
        exit(2);
    }
}




// Arg to pass to sync thread
typedef struct threadArg_t {
    int  socket;
    bool debug;
} threadArg;



/**
 * This thread receives sync messages indicating the latest event sent for a particular data source.
 * @param arg struct to be passed to thread.
 */
static void *syncThread(void *arg) {

    threadArg *tArg = (threadArg *) arg;

    int  socket  = tArg->socket;
    bool debug   = tArg->debug;

    uint32_t version, srcId, evtRate;
    uint64_t evtNum, nanos;
    char pkt[9100];


    while (true) {

        // Read UDP packet
        ssize_t bytesRead = recvfrom(socket, pkt, 9100, 0, nullptr, nullptr);
        if (bytesRead < 20) {
            if (debug) fprintf(stderr, "cp_server: got sync packet that's too small, ignore\n");
            continue;
        }

        // Parse sync msg
        ejfat::parseSyncData(pkt, &version, &srcId, &evtNum, &evtRate, &nanos);

        if (debug) {
            fprintf(stderr, "cp_server: sync pkt from %u, version %u, event #%" PRIu64 ", rate %u Hz, nanos %" PRIu64 "\n",
                    srcId, version, evtNum, evtRate, nanos);
        }
    }

    return nullptr;
}





// structure for passing args to GRPC server thread
typedef struct threadStruct_t {
    LoadBalancerServiceImpl *pGrpcService;
    bool debug;
} threadStruct;


// Thread to monitor all the info coming in from backends and update the control plane
static void *controlThread(void *arg) {

    threadStruct *targ = static_cast<threadStruct *>(arg);
    LoadBalancerServiceImpl *service = targ->pGrpcService;
    int status, fillPercent;
    bool debug = targ->debug;

    int64_t totalT = 0, time;
    struct timespec t1, t2, firstT;

    // random device class instance, source of 'true' randomness for initializing random seed
    std::random_device rd;
    // Mersenne twister PRNG, initialized with seed from previous random device instance
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dist(0.0, 1.0);

    // control (PID error) values from node
    std::map<uint16_t, float> control;
    for(size_t n=0; n < 1024; n++) {control[n] = 0;}
    // schedule density for node
    std::map<uint16_t, float> sched;
    for(size_t n=0; n < 1024; n++) {sched[n] = 0;}
    uint64_t epoch = 0; //for now

    // Get the current time
    clock_gettime(CLOCK_MONOTONIC, &t1);
    firstT = t1;

    while (true) {

        // Delay 2 seconds between data points
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // This needs to be called each loop since it gets a COPY of the current data (for thread safety)
        std::shared_ptr<std::unordered_map<std::string, BackEnd>> pDataMap = service->getBackEnds();
        //number of backends giving feed back this reporting interval
        size_t num_bes = pDataMap->size();

        // Loop over all backends
        for (const std::pair<std::string, BackEnd> &entry: *(pDataMap.get())) {
            const BackEnd &backend = entry.second;

            // read node feedback: an array of health metrics
            uint16_t n = 0;
            control[n] = backend.getPidError();
            sched[n] = sched[n] == 0 ? 1e-6 : sched[n]; //activate node if not active

            if (debug) cout << "Received pid err " << n << ", " << control[n] << " from backend\n";
            if (debug) cout << "sched[" << n << "] = " << sched[n] << " ...\n";

            // update weighting for node from control signal
            sched[n] *= (1.0f + control[n]);
            if (debug) cout << "adjusting sched[" << n << "] = " << sched[n] << " ...\n";
        }

        if (debug) { cout << "read " << num_bes << " controls\n"; }
        if (debug) {
            cout << "control: ";
            for (size_t n = 0; n < num_bes; n++) { cout << control[n] << '\t'; }
            cout << '\n';
        }
        if (debug) { cout << "normalizing ...\n"; }

        // normalize schedule density
        float nrm_sum;
        nrm_sum = 0;
        for (size_t n = 0; n < num_bes; n++) {
            nrm_sum += sched[n];
        }
        nrm_sum = nrm_sum == 0 ? 1 : nrm_sum;
        if (debug) { cout << "nrm_sum = " << nrm_sum << '\n'; }
        // ///////////

        for (size_t n = 0; n < num_bes; n++) {
            sched[n] /= nrm_sum;
        }

        if (debug) {
            cout << "density: ";
            for (size_t n = 0; n < num_bes; n++) { cout << sched[n] << '\t'; }
            cout << '\n';
        }

        if (debug) { cout << "write revised tick schedule ...\n"; }
        // write revised tick schedule
        std::map<uint16_t, uint32_t> lb_calendar_table;

        for (uint16_t t = 0; t < 512; t++) {
            // random # between 0 & 1
            float r = dist(gen);
            if (debug) { cout << "sample = " << r << '\n'; }

            // cumulative distribution from iterating over sched weights
            float cd = 0.f;
            uint16_t n;
            n = 0;
            for (size_t ni = 0; ni < num_bes; ni++) {
                cd += sched[ni];
                if (debug) cout << "testing = " << r << " against " << cd << " n = " << n << '\n';
                if (r <= cd) break;
                n++;
            }

            lb_calendar_table[t] = n;

            if (debug) {
                cout << "sampled index = " << n << '\n';
                cout << "table_add load_balance_calendar_table do_assign_member 0x" << std::hex << epoch << " 0x"
                     << std::hex << t << " => 0x" << std::hex << n << '\n';
            }
        }
    }

    return (nullptr);
}


int main(int argc, char **argv) {

    ssize_t nBytes;
    uint16_t port = 50051, sport = 50052;
    int cores[10];
    bool debug = false;
    bool useIPv6 = false;

    for (int i=0; i < 10; i++) {
        cores[i] = -1;
    }

    parseArgs(argc, argv, cores, &port, &sport, &debug, &useIPv6);

#ifdef __linux__

    if (cores[0] > -1) {
        // Create a cpu_set_t object representing a set of CPUs. Clear it and mark given CPUs as set.
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);

        if (debug) {
            for (int i=0; i < 10; i++) {
                     std::cerr << "core[" << i << "] = " << cores[i] << "\n";
            }
        }

        for (int i=0; i < 10; i++) {
            if (cores[i] >= 0) {
                std::cerr << "Run reassembly thread on core " << cores[i] << "\n";
                CPU_SET(cores[i], &cpuset);
            }
            else {
                break;
            }
        }
        pthread_t current_thread = pthread_self();
        int rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << std::endl;
        }
    }

#endif

    ///////////////////////////////////////////////
    ///    Listening UDP socket for sync msgs   ///
    ///////////////////////////////////////////////

    int udpSocket;


    if (useIPv6) {
        struct sockaddr_in6 serverAddr6{};

        // Create IPv6 UDP socket
        if ((udpSocket = socket(AF_INET6, SOCK_DGRAM, 0)) < 0) {
            perror("creating IPv6 client socket");
            return -1;
        }

        int optval = 1;
        setsockopt(udpSocket, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));

        // Configure settings in address struct
        // Clear it out
        memset(&serverAddr6, 0, sizeof(serverAddr6));
        // it is an INET address
        serverAddr6.sin6_family = AF_INET6;
        // the port we are going to receiver from, in network byte order
        serverAddr6.sin6_port = htons(sport);
        serverAddr6.sin6_addr = in6addr_any;

        // Bind socket with address struct
        int err = bind(udpSocket, (struct sockaddr *) &serverAddr6, sizeof(serverAddr6));
        if (err != 0) {
            if (debug) fprintf(stderr, "bind socket error\n");
            return -1;
        }
    }
    else {
        // Create UDP socket
        if ((udpSocket = socket(PF_INET, SOCK_DGRAM, 0)) < 0) {
            perror("creating IPv4 client socket");
            return -1;
        }

        int optval = 1;
        setsockopt(udpSocket, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));

        // Configure settings in address struct
        struct sockaddr_in serverAddr{};
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(sport);
        serverAddr.sin_addr.s_addr = INADDR_ANY;

        // Bind socket with address struct
        int err = bind(udpSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr));
        if (err != 0) {
            fprintf(stderr, "bind socket error\n");
            return -1;
        }
    }

    /////////////////////////
    /// Start sync Thread ///
    /////////////////////////

    threadArg *tArg = (threadArg *) calloc(1, sizeof(threadArg));
    if (tArg == nullptr) {
        fprintf(stderr, "out of mem\n");
        return -1;
    }

    tArg->socket = udpSocket;
    tArg->debug  = debug;

    pthread_t thd;
    int status = pthread_create(&thd, NULL, syncThread, (void *) tArg);
    if (status != 0) {
        fprintf(stderr, "\n ******* error creating fill thread\n\n");
        return -1;
    }


    ////////////////////////////////
    /// Start GRPC server Thread ///
    ////////////////////////////////

    // Start with offset 0 in very first packet to be read
    uint64_t tick = 0L;
    uint16_t dataId;
    bool firstLoop = true;

    LoadBalancerServiceImpl service;
    LoadBalancerServiceImpl *pGrpcService = &service;

    // Start thread to do run pid loop
    threadStruct *targ = (threadStruct *)calloc(1, sizeof(threadStruct));
    if (targ == nullptr) {
        fprintf(stderr, "out of mem\n");
        return -1;
    }

    targ->debug = debug;
    targ->pGrpcService = pGrpcService;

    pthread_t thd1;
    status = pthread_create(&thd1, NULL, controlThread, (void *) targ);
    if (status != 0) {
        fprintf(stderr, "\n ******* error creating PID thread ********\n\n");
        return -1;
    }

    while (true) {
        std::cout << "About to run GRPC server on port 50051" << std::endl;
        pGrpcService->runServer(port, pGrpcService);
        std::cout << "Should never print this message!!!" << std::endl;
    }

    return 0;
}


