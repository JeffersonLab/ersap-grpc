//
// Created by timmer on 5/19/23.
//

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
 * Send simulated requests/data to the cp_server program (or real CP).
 * Behaves like an ERSAP backend.
 */

#include <cstdlib>
#include <iostream>
#include <time.h>
#include <thread>
#include <cmath>
#include <chrono>
#include <atomic>
#include <algorithm>
#include <cstring>
#include <errno.h>
#include <cinttypes>
#include <memory>
#include <string>
#include <stdexcept>
#include <random>
#include <getopt.h>

#include <atomic>
#include <mutex>
#include <condition_variable>


#ifdef __linux__
#ifndef _GNU_SOURCE
        #define _GNU_SOURCE
    #endif

    #include <sched.h>
    #include <pthread.h>
#endif

// GRPC stuff
#include "lb_cplane.h"
#include "ersap_grpc_assemble.hpp"



template<class X>
X pid(          // Proportional, Integrative, Derivative Controller
        const X& setPoint, // Desired Operational Set Point
        const X& prcsVal,  // Measure Process Value
        const X& delta_t,  // Time delta between determination of last control value
        const X& Kp,       // Konstant for Proprtional Control
        const X& Ki,       // Konstant for Integrative Control
        const X& Kd        // Konstant for Derivative Control
)
{
    static X previous_error = 0; // for Derivative
    static X integral_acc = 0;   // For Integral (Accumulated Error)
    X error = setPoint - prcsVal;
    integral_acc += error * delta_t;
    X derivative = (error - previous_error) / delta_t;
    previous_error = error;
    return Kp * error + Ki * integral_acc + Kd * derivative;  // control output
}



//-----------------------------------------------------------------------
// Using a mutex to change the fifo fill level is similar to the mechanism
// that the ET-fifo system code works when inserting and removing items.
//-----------------------------------------------------------------------
static uint32_t fifoLevel;
static std::mutex fifoMutex;
static std::condition_variable fifoCV;


/**
 * Print out help.
 * @param programName name to use for this program.
 */
static void printHelp(char *programName) {
    fprintf(stderr,
            "\nusage: %s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n\n",
            programName,
            "        [-h] [-v] [-ipv6]",
            "        [-p <data receiving port (for registration, 17750 default)>]",
            "        [-a <data receiving address (for registration)>]",
            "        [-range <data receiving port range (for registration)>]",
            "        [-token <authentication token (for registration)>]",

            "        [-cp_addr <control plane IP address>]",
            "        [-cp_port <control plane port (default 50051)>]",

            "        [-name <backend name>]",
            "        [-id <backend id#>]",

            "        [-b <internal buf size to hold event (150kB default)>]",
            "        [-fifo <fifo size (1000 default)>]",
            "        [-s <PID fifo set point (0 default)>]");

    fprintf(stderr, "        This is a gRPC program that simulates an ERSAP backend by sending messages to a control plane.\n");
    fprintf(stderr, "        The -p, -a, and -range args are only to tell CP where to send our data, but are otherwise unused.\n");
    fprintf(stderr, "        In practice, the buffer into which data is received can expand as needed, so the -b arg gives a value\n");
    fprintf(stderr, "        passed on to the CP which gives the max size of fifo entries as a way for the CP to gauge memory uses.\n");
}


/**
 * Parse all command line options.
 *
 * @param argc          arg count from main().
 * @param argv          arg list from main().
 * @param clientId      filled with id# of this grpc client (backend) to send to control plane.
 * @param setPt         filled with the set point of PID loop used with fifo fill level.
 * @param cpPort        filled with grpc server (control plane) port to info to.
 * @param port          filled with UDP receiving data port to listen on.
 * @param range         filled with range of ports in powers of 2 (entropy).
 * @param listenAddr    filled with IP address to listen on for LB data.
 * @param token         filled with authenication token of backend for CP.
 * @param bufSize       filled with byte size of internal bufs to hold incoming events.
 * @param fifoSize      filled with max fifo size.
 * @param debug         filled with debug flag.
 * @param useIPv6       filled with use IP version 6 flag.
 * @param cpAddr        filled with grpc server (control plane) IP address to info to.
 * @param clientName    filled with name of this grpc client (backend) to send to control plane.
 */
static void parseArgs(int argc, char **argv,
                      uint32_t *clientId, float *setPt, uint16_t *cpPort,
                      uint16_t *port, int *range,
                      char *listenAddr, char *token,
                      uint32_t *bufSize, uint32_t *fifoSize,
                      bool *debug, bool *useIPv6, char *cpAddr, char *clientName) {

    int c, i_tmp;
    bool help = false;
    float sp = 0.;

    /* 4 multiple character command-line options */
    static struct option long_options[] =
            {             {"ipv6",     0, nullptr, 2},
                          {"fifo",     1, nullptr, 3},
                          {"cp_addr",  1, nullptr, 4},
                          {"cp_port",  1, nullptr, 5},
                          {"name",     1, nullptr, 6},
                          {"id",       1, nullptr, 7},
                          {"range",    1, nullptr, 8},
                          {"token",    1, nullptr, 9},
                          {0,       0, 0,    0}
            };


    while ((c = getopt_long_only(argc, argv, "vhs:a:p:b:", long_options, 0)) != EOF) {

        if (c == -1)
            break;

        switch (c) {

            case 'a':
                // LISTENING IP ADDRESS
                if (strlen(optarg) > 15 || strlen(optarg) < 7) {
                    fprintf(stderr, "listening IP address is bad\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                strcpy(listenAddr, optarg);
                break;

            case 'p':
                // Data PORT
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

            case 'b':
                // BUFFER SIZE
                i_tmp = (int)strtol(optarg, nullptr, 0);
                if (i_tmp > 2000000) {
                    *bufSize = 2000000;
                }
                else if (i_tmp >= 1500) {
                    *bufSize = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -b, buf size >= 1500 and <= 2MB\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;


            case 2:
                // use IP version 6
                fprintf(stderr, "SETTING TO IP version 6\n");
                *useIPv6 = true;
                break;

            case 3:
                // max fifo size
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 0 && i_tmp < 100001) {
                    *fifoSize = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -fifo, 0 < size <= 100k\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 8:
                // LB port range
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp >= 0 && i_tmp <= 14) {
                    *range = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -range, 0 <= port <= 14\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 5:
                // grpc server/control-plane PORT
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 1023 && i_tmp < 65535) {
                    *cpPort = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -gport, 1023 < port < 65536\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 's':
                // PID set point for fifo fill
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -s, 0.0 <= PID set point <= 100.0\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                if (sp >= 0. && sp <= 100.) {
                    *setPt = sp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -s, 0 <= PID set point <= 100\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 7:
                // grpc client id
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp >= 0) {
                    *clientId = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -id, backend id must be >= 0\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 4:
                // GRPC server/control-plane IP ADDRESS
                if (strlen(optarg) > 15 || strlen(optarg) < 7) {
                    fprintf(stderr, "grpc server IP address is bad\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                memset(cpAddr, 0, 16);
                strcpy(cpAddr, optarg);
                break;

            case 6:
                // grpc client name
                if (strlen(optarg) > 30 || strlen(optarg) < 1) {
                    fprintf(stderr, "backend name too long/short, %s\n\n", optarg);
                    printHelp(argv[0]);
                    exit(-1);
                }
                strcpy(clientName, optarg);
                break;

            case 9:
                // authentication token
                if (strlen(optarg) > 255 || strlen(optarg) < 1) {
                    fprintf(stderr, "authentication token cannot be blank or > 255 chars, %s\n\n", optarg);
                    printHelp(argv[0]);
                    exit(-1);
                }
                strcpy(token, optarg);
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



// Arg to pass to fifo fill/drain threads
typedef struct threadArg_t {
    // Statistics
    std::shared_ptr<ejfat::packetRecvStats> stats;
    std::shared_ptr<ejfat::queue<std::vector<char>>> sharedQ;
    int  udpSocket;
    uint32_t bufSize;
    bool debug;
} threadArg;



/**
 * This thread receives event over its UDP socket and fills the fifo
 * with these event.
 *
 * @param arg struct to be passed to thread.
 */
static void *fillFifoThread(void *arg) {

    threadArg *tArg = (threadArg *) arg;

    auto stats      = tArg->stats;
    auto sharedQ    = tArg->sharedQ;
    int  udpSocket  = tArg->udpSocket;
    int32_t bufSize = tArg->bufSize;
    bool debug      = tArg->debug;

    uint32_t tickPrescale = 1;
    uint64_t tick;
    uint16_t dataId;
    ssize_t  nBytes;

    clearStats(stats);


    while (true) {
        // Create vector
        std::vector<char> vec;
        // We create vector capacity here, necessary if we're going to use backing array
        // instead of using "push_back()", which we do in the getReassembledBuffer routine.
        vec.reserve(bufSize);

        // Fill vector with data. Insert data about packet order.
        nBytes = getReassembledBuffer(vec, udpSocket, debug, &tick, &dataId, stats, tickPrescale);
        if (nBytes < 0) {
            fprintf(stderr, "Error in getReassembledBuffer, %ld\n", nBytes);
            exit(1);
        }

        // Move this vector into the queue
        sharedQ->push(std::move(vec));
    }

    return nullptr;
}



/**
 * This thread drains the fifo and "processes the data".
 * @param arg struct to be passed to thread.
 */
static void *drainFifoThread(void *arg) {

    threadArg *tArg = (threadArg *) arg;

    auto stats    = tArg->stats;
    auto sharedQ  = tArg->sharedQ;
    bool debug    = tArg->debug;

    uint32_t delay, totalPkts, pktSequence;


    while (true) {
        // Get vector from the queue
        std::vector<char> vec;

        sharedQ->pop(vec);
        char *buf = vec.data();

        ejfat::parsePacketData(buf, &delay, &totalPkts, &pktSequence);

        if (debug) {
            // Print out the packet sequence in the order they arrived.
            // This data was placed into buf in the getReassembledBuffer() routine (see fillFifoThread thread above).
            printf("Pkt delay %u usec, %u total pkts, arrival sequence:\n", delay, totalPkts);

            uint32_t seq;
            for (int i=0; i < totalPkts; i++) {
                seq = buf[12 + 4*i];
                printf(" %u", seq);
            }
            printf("\n");
        }

        // Delay to simulate data processing
        std::this_thread::sleep_for(std::chrono::microseconds(delay));
    }

    return nullptr;
}



int main(int argc, char **argv) {

    ssize_t nBytes;
    uint32_t bufSize = 150000; // 150kB default

    // Set this to max expected data size
    uint32_t clientId = 0;

    float pidError = 0.F;
    float setPoint = 0.F;   // set fifo to 1/2 full by default

    uint16_t cpPort = 50051;
    bool debug = false;
    bool useIPv6 = false;

    int range;
    uint16_t port = 17750;

    uint32_t fifoCapacity = 1000;

    char cpAddr[16];
    memset(cpAddr, 0, 16);
    strcpy(cpAddr, "172.19.22.15"); // ejfat-4 by default

    char clientName[31];
    memset(clientName, 0, 31);

    char listeningAddr[16];
    memset(listeningAddr, 0, 16);

    char authToken[256];
    memset(authToken, 0, 256);

    parseArgs(argc, argv, &clientId, &setPoint, &cpPort, &port, &range,
              listeningAddr, authToken, &bufSize, &fifoCapacity, &debug, &useIPv6, cpAddr,  clientName);

    // give it a default name
    if (strlen(clientName) < 1) {
        std::string name = "backend" + std::to_string(clientId);
        std::strcpy(clientName, name.c_str());
    }

    // convert integer range in PortRange enum-
    auto pRange = PortRange(range);

    ///////////////////////////////////
    ///    Listening UDP socket    ///
    //////////////////////////////////

    int udpSocket;
    int recvBufSize = 25000000;

    if (useIPv6) {
        struct sockaddr_in6 serverAddr6{};

        // Create IPv6 UDP socket
        if ((udpSocket = socket(AF_INET6, SOCK_DGRAM, 0)) < 0) {
            perror("creating IPv6 client socket");
            return -1;
        }

        // Set & read back UDP receive buffer size
        socklen_t size = sizeof(int);
        setsockopt(udpSocket, SOL_SOCKET, SO_RCVBUF, &recvBufSize, sizeof(recvBufSize));
        recvBufSize = 0;
        getsockopt(udpSocket, SOL_SOCKET, SO_RCVBUF, &recvBufSize, &size);
        if (debug) fprintf(stderr, "UDP socket recv buffer = %d bytes\n", recvBufSize);

        int optval = 1;
        setsockopt(udpSocket, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));

        // Configure settings in address struct
        // Clear it out
        memset(&serverAddr6, 0, sizeof(serverAddr6));
        // it is an INET address
        serverAddr6.sin6_family = AF_INET6;
        // the port we are going to receiver from, in network byte order
        serverAddr6.sin6_port = htons(port);
        if (strlen(listeningAddr) > 0) {
            inet_pton(AF_INET6, listeningAddr, &serverAddr6.sin6_addr);
        }
        else {
            serverAddr6.sin6_addr = in6addr_any;
        }

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

        // Set & read back UDP receive buffer size
        socklen_t size = sizeof(int);
        setsockopt(udpSocket, SOL_SOCKET, SO_RCVBUF, &recvBufSize, sizeof(recvBufSize));
        recvBufSize = 0;
        getsockopt(udpSocket, SOL_SOCKET, SO_RCVBUF, &recvBufSize, &size);
        fprintf(stderr, "UDP socket recv buffer = %d bytes\n", recvBufSize);

        int optval = 1;
        setsockopt(udpSocket, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));

        // Configure settings in address struct
        struct sockaddr_in serverAddr{};
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        serverAddr.sin_port = htons(port);
        if (strlen(listeningAddr) > 0) {
            serverAddr.sin_addr.s_addr = inet_addr(listeningAddr);
        }
        else {
            serverAddr.sin_addr.s_addr = INADDR_ANY;
        }
        memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);

        // Bind socket with address struct
        int err = bind(udpSocket, (struct sockaddr *) &serverAddr, sizeof(serverAddr));
        if (err != 0) {
            fprintf(stderr, "bind socket error\n");
            return -1;
        }
    }

    ///////////////////////////////////
    /// Start Fill & Drain Threads ///
    //////////////////////////////////

    // Statistics
    std::shared_ptr<ejfat::packetRecvStats> stats = std::make_shared<ejfat::packetRecvStats>();

    // Fifo/queue in which to hold reassembled buffers
    auto sharedQ = std::make_shared<ejfat::queue<std::vector<char>>>(fifoCapacity);


    threadArg *targ = (threadArg *) calloc(1, sizeof(threadArg));
    if (targ == nullptr) {
        fprintf(stderr, "out of mem\n");
        return -1;
    }

    targ->stats = stats;
    targ->sharedQ = sharedQ;
    targ->udpSocket = udpSocket;
    targ->bufSize = bufSize;
    targ->debug = debug;

    pthread_t thdFill;
    int status = pthread_create(&thdFill, NULL, fillFifoThread, (void *) targ);
    if (status != 0) {
        fprintf(stderr, "\n ******* error creating fill thread\n\n");
        return -1;
    }

    pthread_t thdDrain;
    status = pthread_create(&thdDrain, NULL, drainFifoThread, (void *) targ);
    if (status != 0) {
        fprintf(stderr, "\n ******* error creating drain thread\n\n");
        return -1;
    }

    ////////////////////////////
    /// Control Plane  Stuff ///
    ////////////////////////////

    LoadBalancerServiceImpl service;
    LoadBalancerServiceImpl *pGrpcService = &service;

    // PID loop variables
    const float Kp = 0.5;
    const float Ki = 0.0;
    const float Kd = 0.00;
    const float deltaT = 1.0; // 1 millisec

    // Create grpc client of control plane
    LbControlPlaneClient client(cpAddr, cpPort,
                                listeningAddr, port, pRange,
                                clientName, authToken,
                                bufSize, fifoCapacity, setPoint);

    // Register this client with the grpc server
    int32_t err = client.Register();
    if (err == -1) {
        printf("GRPC client %s is already registered!\n", clientName);
    }
    else if (err == 1) {
        printf("GRPC client %s communication error with server when registering, exit!\n", clientName);
        exit(1);
    }

    // Add stuff to prevent anti-aliasing.
    // If sampling every millisec, an individual reading sent every 1 sec
    // will NOT be an accurate representation. It will include a lot of noise. To prevent this,
    // keep a running average of the fill %, so its reported value is a more accurate portrayal
    // of what's really going on. In this case a running avg is taken over the reporting time.

    int loopMax   = 1000;
    int loopCount = loopMax;  // loopMax loops of waitMicroSecs microseconds
    int waitMicroSecs = 1000; // By default, loop every millisec

    float runningFillTotal = 0., fillAvg;
    float fillValues[loopMax];
    memset(fillValues, 0, loopMax*sizeof(float));

    // Keep circulating thru array. Highest index is loopMax - 1.
    // The first time thru, we don't want to over-weight with (loopMax - 1) zero entries.
    // So we read loopMax entries first, before we start keeping stats & reporting level.
    float prevFill, curFill, fillPercent;
    bool startingUp = true;
    int fillIndex = 0, firstLoopCounter = 1;


    while (true) {

        // Delay between data points
        std::this_thread::sleep_for(std::chrono::microseconds(waitMicroSecs));

        // Read current fifo level
        curFill = sharedQ->size();
        // Previous value at this index
        prevFill = fillValues[fillIndex];
        // Store current val at this index
        fillValues[fillIndex++] = curFill;
        // Add current val and remove previous val at this index from the running total.
        // That way we have added loopMax number of most recent entries at ony one time.
        runningFillTotal += curFill - prevFill;
        // Find index for the next round
        fillIndex = (fillIndex == loopMax) ? 0 : fillIndex;

        if (startingUp) {
            if (firstLoopCounter++ >= loopMax) {
                // Done with first loopMax loops
                startingUp = false;
            }
            else {
                // Don't start sending data or recording values
                // until the startup time (loopMax loops) is over.
                continue;
            }
            fillAvg = runningFillTotal / loopMax;
        }
        else {
            fillAvg = runningFillTotal / loopMax;
        }

        fillPercent = fillAvg/fifoCapacity*100;

        // PID error
        pidError = pid(setPoint, fillPercent, deltaT, Kp, Ki, Kd);

        // Every "loopMax" loops
        if (--loopCount <= 0) {
            // Update the changing variables
            client.update(fillAvg, pidError);

            // Send to server
            err = client.SendState();
            if (err == 1) {
                printf("GRPC client %s communication error with server during sending of data!\n", clientName);
                break;
            }

            printf("Fifo %d%% filled, %d avg level, pid err %f\n", (int)fillPercent, (int)fillAvg, pidError);

            loopCount = loopMax;
        }
    }

    // Unregister this client with the grpc server
    err = client.Deregister();
    if (err == 1) {
        printf("GRPC client %s communication error with server when unregistering, exit!\n", clientName);
    }
    exit(1);


    return 0;
}
