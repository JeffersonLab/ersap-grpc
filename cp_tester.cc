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
 * Send simulated requests/data to the CP.
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

//#include <atomic>


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
X pid(                      // Proportional, Integrative, Derivative Controller
        const X& setPoint,  // Desired Operational Set Point
        const X& prcsVal,   // Measure Process Value
        const X& delta_t,   // Time delta between determination of last control value
        const X& Kp,        // Konstant for Proprtional Control
        const X& Ki,        // Konstant for Integrative Control
        const X& Kd,        // Konstant for Derivative Control
        const X& prev_err,  // previous error
        const X& prev_err_t // # of microseconds earlier that previous error was recorded
)
{
    static X integral_acc = 0;   // For Integral (Accumulated Error)
    X error = setPoint - prcsVal;
    integral_acc += error * delta_t;
    //X derivative = (error - prev_err) / delta_t; // delta_t = 1 sec since this previous_error
    X derivative = (error - prev_err) * 1000000. / prev_err_t;
//    if (prev_err_t == 0 || prev_err_t != prev_err_t) {
//        derivative = 0;
//    }

    return Kp * error + Ki * integral_acc + Kd * derivative;  // control output
}



//-----------------------------------------------------------------------
// Using a mutex to change the fifo fill level is similar to the mechanism
// that the ET-fifo system code works when inserting and removing items.
//-----------------------------------------------------------------------
static uint32_t fifoLevel;
static std::mutex fifoMutex;
static std::condition_variable fifoCV;

// Keep track of counters
static uint64_t eventsReassembled = 0;
static uint64_t eventsProcessed = 0;


/**
 * Print out help.
 * @param programName name to use for this program.
 */
static void printHelp(char *programName) {
    fprintf(stderr,
            "\nusage: %s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n\n",
            programName,
            "        [-h] [-v] [-ipv6]",
            "        [-p <data receiving port (for registration, 17750 default)>]",
            "        [-a <data receiving address (for registration)>]",
            "        [-range <data receiving port range (for registration), default 0>]",
            "        [-token <administrative token (for registration, default = udplbd_default_change_me)>]",
            "        [-file <fileName to hold output>]\n",

            "        [-cp_addr <control plane IP address (default ejfat-2)>]",
            "        [-cp_port <control plane grpc port (default 18347)>]",
            "        [-name <backend name>]\n",
            "        [-w <weight relative to other backends (default 1.)>]\n",
            "        [-lbid <id of LB to use (default LB_0)>]\n",

            "        [-count <# of fill values averaged, default = 1000>]",
            "        [-rtime <millisec for reporting fill to CP, default 1000>]",
            "        [-stime <fifo sample time in millisec, default 1>]",
            "        [-factor <real # to multiply process time of event, default 1., > 0.]",
            "        [-thds <# of threads which consume events off Q, default 1, max 12>]\n",

            "        [-b <internal buf size to hold event (150kB default)>]",
            "        [-cores <comma-separated list of cores to run on>]",
            "        [-fifo <fifo size (1000 default)>]",
            "        [-s <PID fifo set point (0 default)>]",
            "        [-pid <set max EPR in Hz (min 1) and have PID control on relative incoming rate>]",
            "        [-fill <set reported fifo fill %, 0-1 (and pid error to 0) for testing>]\n",

            "        [-Kp <proportional gain (0.52 default)>]",
            "        [-Ki <integral gain (0.005 default)>]",
            "        [-Kd <derivative gain (0.0 default)>]",
            "        [-csv <path (stdout default)>]");

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
 * @param cores         array of core ids on which to run assembly thread.
 * @param setPt         filled with the set point of PID loop used with fifo fill level.
 * @param cpPort        filled with grpc server (control plane) port to info to.
 * @param port          filled with UDP receiving data port to listen on.
 * @param range         filled with range of ports in powers of 2 (entropy).
 * @param listenAddr    filled with IP address to listen on for LB data.
 * @param token         filled with authenication token of backend for CP.
 * @param filename      filled with name of file to hold program output instead of stdout.
 * @param csvFileName   filled with name of file to hold various program data.
 * @param bufSize       filled with byte size of internal bufs to hold incoming events.
 * @param fifoSize      filled with max fifo size.
 * @param fillCount     filled with # of fill level measurements to average together before sending.
 * @param reportTime    filled with millisec between reports to CP.
 * @param sampleTime    filled with millisec between reports to CP.
 * @param processThds   filled with # of threads which pull reassembled events off of Q and "process".
 * @param lbid          filled with ID of the LB to be used.
 * @param debug         filled with debug flag.
 * @param useIPv6       filled with use IP version 6 flag.
 * @param cpAddr        filled with grpc server (control plane) IP address to info to.
 * @param clientName    filled with name of this grpc client (backend) to send to control plane.
 * @param kp            filled with PID proportional constant.
 * @param ki            filled with PID integral constant.
 * @param kd            filled with PID differential constant.
 * @param fill          filled with fixed value to report as fifo fill level (0-1).
 * @param ffactor       filled with fudge factor to multiply event processing time with.
 * @param maxEPR        filled with max event processing rate for node and have PID key on relative incoming ev rate.
 * @param weight        filled with weight of this relative to other backends for the given LB.
 */
static void parseArgs(int argc, char **argv,
                      int *cores, float *setPt, uint16_t *cpPort,
                      uint16_t *port, int *range,
                      char *listenAddr, char *token,
                      char *fileName, char *csvFileName,
                      uint32_t *bufSize, uint32_t *fifoSize,
                      uint32_t *fillCount, int32_t *reportTime,
                      int32_t *sampleTime, uint32_t *processThds,
                      bool *debug, bool *useIPv6,
                      char *cpAddr, char *clientName, char *lbid,
                      float *kp, float *ki, float *kd,
                      float *fill, float *ffactor, float *maxEPR, float *weight) {

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
                          {"cores",    1, nullptr, 7},
                          {"range",    1, nullptr, 8},
                          {"token",    1, nullptr, 9},
                          {"file",     1, nullptr, 10},
                          {"Kp",       1, nullptr, 11},
                          {"Ki",       1, nullptr, 12},
                          {"Kd",       1, nullptr, 13},
                          {"count",    1, nullptr, 14},
                          {"rtime",    1, nullptr, 15},
                          {"fill",     1, nullptr, 16},
                          {"csv",      1, nullptr, 17},
                          {"thds",     1, nullptr, 18},
                          {"factor",   1, nullptr, 19},
                          {"stime",    1, nullptr, 20},
                          {"pid",      1, nullptr, 21},
                          {"lbid",     1, nullptr, 22},
                          {0,         0, 0,    0}
            };


    while ((c = getopt_long_only(argc, argv, "vhs:a:p:b:w:", long_options, 0)) != EOF) {

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

            case 'w':
                // weight
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -w\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                if (sp < 1.F) {
                    fprintf(stderr, "Values to -w must be >= 0\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                *weight = sp;
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

            case 7:
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

            case 's':
                // PID set point for fifo fill
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -s, 0.0 <= PID set point <= 1.0\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                if (sp >= 0. && sp <= 1.) {
                    *setPt = sp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -s, 0.0 <= PID set point <= 1.0\n\n");
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

            case 10:
                // file name
                if (strlen(optarg) > 128 || strlen(optarg) < 1) {
                    fprintf(stderr, "filename too long/short, %s\n\n", optarg);
                    printHelp(argv[0]);
                    exit(-1);
                }
                strcpy(fileName, optarg);
                break;

            case 11:
                // Set the proportional gain Kp for the PID loop
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -Kp\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                *kp = sp;
                break;

            case 12:
                 // Set the integral gain Ki for the PID loop
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -Ki\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                *ki = sp;
                break;

            case 13:
                 // Set the derivative gain Kd for the PID loop
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -Kd\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                *kd = sp;
                break;

            case 14:
                // count = # of fill level values averaged together before reporting
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 0) {
                    *fillCount = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -count, must be > 0\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 15:
                // reporting interval in millisec
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 0) {
                    *reportTime = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -rtime, must be >= 1 ms\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 16:
                // Fix the reported fifo fill level (0-1) to this value - for testing
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -fill\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                if (sp > 1.F || sp < 0.F) {
                    fprintf(stderr, "Values to -fill must be >= 0. and <= 1.\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                *fill = sp;
                break;

            case 17:
                // Set the CSV output path
                if (strlen(optarg) > 128 || strlen(optarg) < 1) {
                    fprintf(stderr, "filename too long/short, %s\n\n", optarg);
                    printHelp(argv[0]);
                    exit(-1);
                }
                strcpy(csvFileName, optarg);
                break;

            case 18:
                // # of event processing threads
                i_tmp = (int)strtol(optarg, nullptr, 0);
                if (i_tmp > 12) {
                    *processThds = 12;
                }
                else if (i_tmp > 0) {
                    *processThds = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -thds, # thds > 0 and <= 12\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 19:
                // Processing time fudge factor
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -factor\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                if (sp < 0.F) {
                    fprintf(stderr, "Values to -factor must be > 0\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                *ffactor = sp;
                break;

            case 20:
                // fifo sampling time in millisec , change to microsec
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 0) {
                    *sampleTime = 1000*i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -stime, must be >= 1 ms\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 21:
                // set max event processing rate in Hz and use relative incoming ev rate for PID control
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -pid\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                if (sp < 1.F) {
                    fprintf(stderr, "Values to -pid must be >= 1.\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }

                *maxEPR = sp;
                break;

            case 22:
                // Load Balancer ID
                if (strlen(optarg) > 255 || strlen(optarg) < 1) {
                    fprintf(stderr, "LB id cannot be blank or > 255 chars, %s\n\n", optarg);
                    printHelp(argv[0]);
                    exit(-1);
                }
                strcpy(lbid, optarg);
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
    else if (1000*(*reportTime) < *sampleTime) {
        fprintf(stderr, "Sample time must be <= reporting time\n\n");
        printHelp(argv[0]);
        exit(-1);
    }
    else if (1000*(*reportTime) % *sampleTime != 0) {
        fprintf(stderr, "Reporting time must be integer multiple of sample time\n\n");
        printHelp(argv[0]);
        exit(-1);
    }

}


// Statistics
static std::atomic_int64_t totalBytes{0}, totalPackets{0}, totalEvents{0};
static std::atomic_int64_t droppedPackets{0}, droppedEvents{0}, droppedBytes{0};
static std::atomic_int64_t discardedBuiltPkts{0}, discardedBuiltEvts{0}, discardedBuiltBytes{0};
static std::atomic_int processThdId {0};



// Arg to pass to fifo fill/drain threads
typedef struct threadArg_t {
    // Statistics
    std::shared_ptr<ejfat::packetRecvStats> stats;
    std::shared_ptr<ejfat::queue<std::vector<char>>> sharedQ;
    int  udpSocket;
    int  *cores; // array of cores to run on
    uint32_t bufSize;
    bool debug;
    bool writeToFile;
    float ffactor;
    FILE *fp;
} threadArg;



/**
 * This thread receives event over its UDP socket and fills the fifo
 * with these event.
 *
 * @param arg struct to be passed to thread.
 */
static void *fillFifoThread(void *arg) {

    threadArg *tArg = (threadArg *) arg;

    auto stats       = tArg->stats;
    auto sharedQ     = tArg->sharedQ;
    int  udpSocket   = tArg->udpSocket;
    int32_t bufSize  = tArg->bufSize;
    bool writeToFile = tArg->debug;
    bool debug       = tArg->debug;
    int *cores       = tArg->cores;
    FILE *fp         = tArg->fp;

    uint32_t tickPrescale = 1;
    uint64_t tick;
    uint16_t dataId;
    ssize_t  nBytes;

    clearStats(stats);
    int64_t prevTotalPackets;

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

    while (true) {
        // Create vector
        std::vector<char> vec;
        // We create vector capacity here, necessary if we're going to use backing array
        // instead of using "push_back()", which we do in the getReassembledBuffer routine.
        vec.reserve(bufSize);

        // We do NOT know what the expected tick value is to be received since the LB can mix it up.
        // The following value keeps getReassembledBuffer from calculating dropped events & pkts
        // based on the expected tick value.
        tick = 0xffffffffffffffffL;

        // Store values before reassembly in case we need to dump buffer if fifo full
        prevTotalPackets = totalPackets;

        // Fill vector with data. Insert data about packet order.
        nBytes = getReassembledBuffer(vec, udpSocket, debug, &tick, &dataId, stats, tickPrescale);
        if (nBytes < 0) {
            if (writeToFile) fprintf(fp, "Error in getReassembledBuffer, %ld\n", nBytes);
            perror("Error in getReassembledBuffer");
            exit(1);
        }

        // Receiving Stats

        // stats keeps a running total in which getReassembledBuffer adds to it with each call
        totalBytes  += nBytes;
        totalPackets = stats->acceptedPackets;
        totalEvents++;
        eventsReassembled++;

        droppedBytes   = stats->discardedBytes;
        droppedEvents  = stats->discardedBuffers;
        droppedPackets = stats->discardedPackets;

        // Move this vector into the queue, but don't block.
        if (!sharedQ->try_push(std::move(vec))) {
            // If the Q full, dump event and move on,
            // which is what happens with Vardan's backend and the ET system.
            // So hopefully this is a decent model of the backend.

            // Track what is specifically dumped due to full Q
            discardedBuiltEvts++;
            discardedBuiltPkts  += stats->acceptedPackets - prevTotalPackets;
            discardedBuiltBytes += nBytes;
        }
    }

    return nullptr;
}



/**
 * This thread drains the fifo and "processes the data".
 * @param arg struct to be passed to thread.
 */
static void *drainFifoThread(void *arg) {

    int id = processThdId.fetch_add(1);
    std::cerr << "Running drain thread " << id << std::endl;

    threadArg *tArg = (threadArg *) arg;

    auto sharedQ  = tArg->sharedQ;
    bool debug    = tArg->debug;
    float ffactor = tArg->ffactor;
    FILE *fp      = tArg->fp;

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
            fprintf(fp, "Thd %d: pkt delay %u usec, %u total pkts, arrival sequence:\n", id, delay, totalPkts);

            uint32_t seq;
            for (int i=0; i < totalPkts; i++) {
                seq = buf[12 + 4*i];
                fprintf(fp, " %u", seq);
            }
            fprintf(fp, "\n");
        }

        // Delay to simulate data processing. Fudge factor can tweak it to make machine look slower or faster
        delay = (uint32_t) ((float)delay * ffactor);
        std::this_thread::sleep_for(std::chrono::microseconds(delay));
        eventsProcessed++;
    }

    return nullptr;
}


// Thread to send to print out rates
static void *rateThread(void *arg) {

    int64_t packetCount, byteCount, eventCount;
    int64_t prevTotalPackets, prevTotalBytes, prevTotalEvents;
    int64_t currTotalPackets, currTotalBytes, currTotalEvents;

    int64_t dropPacketCount, dropByteCount, dropEventCount;
    int64_t currDropTotalPackets, currDropTotalBytes, currDropTotalEvents;
    int64_t prevDropTotalPackets, prevDropTotalBytes, prevDropTotalEvents;

    int64_t builtDisPacketCount, builtDisByteCount, builtDisEventCount;
    int64_t currBuiltDisTotPkts, currBuiltDisTotBytes, currBuiltDisTotEvts;
    int64_t prevBuiltDisTotPkts, prevBuiltDisTotBytes, prevBuiltDisTotEvts;

    // Ignore first rate calculation as it's most likely a bad value
    bool skipFirst = true;

    double pktRate, pktAvgRate, dataRate, dataAvgRate, totalRate, totalAvgRate, evRate, avgEvRate;
    int64_t totalT = 0, time, absTime;
    struct timespec t1, t2, firstT;

    // Get the current time
    clock_gettime(CLOCK_MONOTONIC, &t1);
    firstT = t1;

    while (true) {

        prevTotalBytes   = totalBytes;
        prevTotalPackets = totalPackets;
        prevTotalEvents  = totalEvents;

        prevDropTotalBytes   = droppedBytes;
        prevDropTotalPackets = droppedPackets;
        prevDropTotalEvents  = droppedEvents;

        prevBuiltDisTotBytes = discardedBuiltBytes;
        prevBuiltDisTotPkts  = discardedBuiltPkts;
        prevBuiltDisTotEvts  = discardedBuiltEvts;

        // Delay 4 seconds between printouts
        std::this_thread::sleep_for(std::chrono::seconds(4));

        // Read time
        clock_gettime(CLOCK_MONOTONIC, &t2);
        // Epoch time in milliseconds
        absTime = 1000L*(t2.tv_sec) + (t2.tv_nsec)/1000000L;
        // time diff in microseconds
        time = (1000000L * (t2.tv_sec - t1.tv_sec)) + ((t2.tv_nsec - t1.tv_nsec)/1000L);
        totalT = (1000000L * (t2.tv_sec - firstT.tv_sec)) + ((t2.tv_nsec - firstT.tv_nsec)/1000L);

        currTotalBytes   = totalBytes;
        currTotalPackets = totalPackets;
        currTotalEvents  = totalEvents;

        currDropTotalBytes   = droppedBytes;
        currDropTotalPackets = droppedPackets;
        currDropTotalEvents  = droppedEvents;

        currBuiltDisTotBytes = discardedBuiltBytes;
        currBuiltDisTotPkts  = discardedBuiltPkts;
        currBuiltDisTotEvts  = discardedBuiltEvts;

        if (skipFirst) {
            // Don't calculate rates until data is coming in
            if (currTotalPackets > 0) {
                skipFirst = false;
            }
            else {
                firstT = t1 = t2;
                totalT = totalBytes = totalPackets = totalEvents = 0;
                droppedBytes = droppedPackets = droppedEvents = 0;
                discardedBuiltBytes = discardedBuiltEvts = discardedBuiltPkts = 0;
//printf("Stats, reset totalEvent to 0 since data is not coming in\n");
                continue;
            }
        }

        // Use for instantaneous rates
        byteCount   = currTotalBytes   - prevTotalBytes;
        packetCount = currTotalPackets - prevTotalPackets;
        eventCount  = currTotalEvents  - prevTotalEvents;

        dropByteCount   = currDropTotalBytes   - prevDropTotalBytes;
        dropPacketCount = currDropTotalPackets - prevDropTotalPackets;
        dropEventCount  = currDropTotalEvents  - prevDropTotalEvents;

        builtDisByteCount   = currBuiltDisTotBytes - prevBuiltDisTotBytes;
        builtDisPacketCount = currBuiltDisTotPkts  - prevBuiltDisTotPkts;
        builtDisEventCount  = currBuiltDisTotEvts  - prevBuiltDisTotEvts;

        // Reset things if #s rolling over
        if ( (byteCount < 0) || (totalT < 0) )  {
            totalT = totalBytes = totalPackets = totalEvents = 0;
            droppedBytes = droppedPackets = droppedEvents = 0;
            firstT = t1 = t2;
//printf("Stats, reset totalEvent to 0 since byteCount or totalT rolled over\n");
            continue;
        }

        pktRate = 1000000.0 * ((double) packetCount) / time;
        pktAvgRate = 1000000.0 * ((double) currTotalPackets) / totalT;
        printf("Packets:       %3.4g Hz,    %3.4g Avg, time: diff = %" PRId64 " usec\n",
                pktRate, pktAvgRate, time);


        // Data rates (with NO header info)
        dataRate = ((double) byteCount) / time;
        dataAvgRate = ((double) currTotalBytes) / totalT;
        // Data rates (with RE header info)
        totalRate = ((double) (byteCount + HEADER_BYTES*packetCount)) / time;
        totalAvgRate = ((double) (currTotalBytes + HEADER_BYTES*currTotalPackets)) / totalT;
        printf("Data (+hdrs):  %3.4g (%3.4g) MB/s,  %3.4g (%3.4g) Avg\n", dataRate, totalRate, dataAvgRate, totalAvgRate);

        // Event rates
        evRate = 1000000.0 * ((double) eventCount) / time;
        avgEvRate = 1000000.0 * ((double) currTotalEvents) / totalT;
        printf("Events:        %3.4g Hz,  %3.4g Avg, total %" PRIu64 "\n", evRate, avgEvRate, totalEvents.load());

        // Drop info
        printf("Dropped:       %" PRId64 ", (%" PRId64 " total) evts,   pkts: %" PRId64 ", %" PRId64 " total\n",
                dropEventCount, currDropTotalEvents, dropPacketCount, currDropTotalPackets);

        printf("FullQ Discard: %" PRId64 ", (%" PRId64 " total) evts,   pkts: %" PRId64 ", %" PRId64 " total\n\n",
                builtDisEventCount, currBuiltDisTotEvts, builtDisPacketCount, currBuiltDisTotPkts);

        t1 = t2;
    }

    return (nullptr);
}


int main(int argc, char **argv) {

    ssize_t nBytes;
    uint32_t bufSize = 150000; // 150kB default
    int cores[10];

    float setFill   = -1.0F;
    float setPoint  = 0.F;   // set fifo to 1/2 full by default
    float ffactor   = 1.F;
    float maxEPR    = 0.F;   // Max EPR set on command line
    float weight    = 1.F;   // Weight of this BE compared to others for given LB

    uint16_t cpPort = 18347;
    bool debug = false;
    bool useIPv6 = false;
    bool writeToFile = false;
    bool writeToCsvFile = false;
    bool fixedFill = false;
    bool usePidEpr = false;

    int range = 0;
    uint16_t port = 17750;

    uint32_t fifoCapacity = 1000;
    float    fifoCapacityFlt;

    // PID loop variables
    float Kp = 0.52;
    float Ki = 0.005;
    float Kd = 0.000;

    // # of fill values to average when reporting to grpc
    uint32_t fcount = 1000;
    float    fcountFlt;
    // time period in millisec for reporting to CP
    int32_t reportTime = 1000;
    // time period in microsec for sampling fifo
    int32_t sampleTime = 1000;
    // # thds to process reassembled events
    uint32_t processThds = 1;

    char cpAddr[16];
    memset(cpAddr, 0, 16);
    strcpy(cpAddr, "129.57.177.144"); // ejfat-2 by default

    char clientName[31];
    memset(clientName, 0, 31);

    char listeningAddr[16];
    memset(listeningAddr, 0, 16);

    char fileName[128];
    memset(fileName, 0, 128);

    char csvFileName[128];
    memset(fileName, 0, 128);

    char adminToken[256];
    memset(adminToken, 0, 256);
    strcpy(adminToken, "udplbd_default_change_me");

    // ID of LB to be used
    char lbid[256];
    memset(lbid, 0, 256);
    strcpy(lbid, "LB_0");

    for (int i=0; i < 10; i++) {
        cores[i] = -1;
    }

    parseArgs(argc, argv, cores, &setPoint, &cpPort, &port, &range,
              listeningAddr, adminToken, fileName, csvFileName,
              &bufSize, &fifoCapacity, &fcount, &reportTime,
              &sampleTime, &processThds,
              &debug, &useIPv6, cpAddr,  clientName, lbid,
              &Kp, &Ki, &Kd, &setFill, &ffactor, &maxEPR, &weight);

    // give it a default name
    if (strlen(clientName) < 1) {
        // tack on int which is lowest 16 bits of current time
        time_t localT = time(nullptr) & 0xffff;
        std::string name = "backend" + std::to_string(localT);
        std::strcpy(clientName, name.c_str());
    }

    // Don't compare floats directly
    if (setFill >= -0.1F) {
        fprintf(stderr, "sending CP fixed fifo fill = %f, pid error = 0\n", setFill);
        fixedFill = true;
    }

    // convert integer range in PortRange enum-
    auto pRange = PortRange(range);

    // Change to floats for later computations
    fifoCapacityFlt = static_cast<float>(fifoCapacity);
    fcountFlt = static_cast<float>(fcount);

    if (maxEPR > 0.) {
        // Have pid controling on incoming-event-rate / max-EPR
        fprintf(stderr, "using event rate as PID loop variable, Max EPR = %.2f\n", maxEPR);
        usePidEpr = true;
    }

    ///////////////////////////////////
    ///       output to file(s)     ///
    ///////////////////////////////////

    // By default, output goes to stderr
    FILE *fp = stderr;
    FILE *csvFp = stdout;

    // else send to file
    if (strlen(fileName) > 0) {
        fp = fopen(fileName, "w");
        if (!fp) {
            fprintf(stderr, "file open failed: %s\n", strerror(errno));
            return(1);
        }
        writeToFile = true;
    }

    if (strlen(csvFileName) > 0) {
        fp = fopen(csvFileName, "w");
        if (!fp) {
            fprintf(stderr, "csv file open failed: %s\n", strerror(errno));
            return(1);
        }
        writeToCsvFile = true;
    }

    ///////////////////////////////////
    ///    Listening UDP socket     ///
    ///////////////////////////////////

    int udpSocket;
    int recvBufSize = 25000000;

    if (useIPv6) {
        struct sockaddr_in6 serverAddr6{};

        // Create IPv6 UDP socket
        if ((udpSocket = socket(AF_INET6, SOCK_DGRAM, 0)) < 0) {
            if (writeToFile) fprintf(fp, "error creating IPv6 client socket\n");
            perror("creating IPv6 client socket");
            return(1);
        }

        // Set & read back UDP receive buffer size
        socklen_t size = sizeof(int);
        setsockopt(udpSocket, SOL_SOCKET, SO_RCVBUF, &recvBufSize, sizeof(recvBufSize));
        recvBufSize = 0;
        getsockopt(udpSocket, SOL_SOCKET, SO_RCVBUF, &recvBufSize, &size);
        if (debug) fprintf(fp, "UDP socket recv buffer = %d bytes\n", recvBufSize);

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
            if (writeToFile) fprintf(fp, "error binding socket\n");
            perror("bind socket error");
            return(1);
        }
    }
    else {
        // Create UDP socket
        if ((udpSocket = socket(PF_INET, SOCK_DGRAM, 0)) < 0) {
            if (writeToFile) fprintf(fp, "error creating IPv4 client socket\n");
            perror("creating IPv4 client socket");
            return(1);
        }

        // Set & read back UDP receive buffer size
        socklen_t size = sizeof(int);
        setsockopt(udpSocket, SOL_SOCKET, SO_RCVBUF, &recvBufSize, sizeof(recvBufSize));
        recvBufSize = 0;
        getsockopt(udpSocket, SOL_SOCKET, SO_RCVBUF, &recvBufSize, &size);
        fprintf(fp, "UDP socket recv buffer = %d bytes\n", recvBufSize);

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
            if (writeToFile) fprintf(fp, "error binding socket\n");
            perror("bind socket error");
            return(1);
        }
    }

    ///////////////////////////////////
    /// Start Stat Thread          ///
    //////////////////////////////////

    // Start thread to printout incoming data rate
    pthread_t thd;
    int status = pthread_create(&thd, NULL, rateThread, (void *) nullptr);
    if (status != 0) {
        if (writeToFile) fprintf(fp, "cannot start statistics thread\n");
        perror("cannot start stat thd");
        return(1);
    }

    ///////////////////////////////////
    ///     Start Fill Thread      ///
    //////////////////////////////////

    // Statistics
    std::shared_ptr<ejfat::packetRecvStats> stats = std::make_shared<ejfat::packetRecvStats>();

    // Fifo/queue in which to hold reassembled buffers
    auto sharedQ = std::make_shared<ejfat::queue<std::vector<char>>>(fifoCapacity);


    threadArg *targ = (threadArg *) calloc(1, sizeof(threadArg));
    if (targ == nullptr) {
        if (writeToFile) fprintf(fp, "out of mem\n");
        perror("out of mem");
        return(1);
    }

    targ->stats = stats;
    targ->bufSize = bufSize;
    targ->sharedQ = sharedQ;
    targ->udpSocket = udpSocket;
    targ->writeToFile = writeToFile;
    targ->debug = debug;
    targ->cores = cores;
    targ->fp = fp;
    targ->ffactor = ffactor;

    pthread_t thdFill;
    status = pthread_create(&thdFill, NULL, fillFifoThread, (void *) targ);
    if (status != 0) {
        if (writeToFile) fprintf(fp, "error creating fill thread\n");
        perror("error creating fill thread");
        return(1);
    }

    ///////////////////////////////////
    ///    Start Drain Threads     ///
    //////////////////////////////////

    for (int i=0; i < processThds; i++) {
        pthread_t thdDrain;
        status = pthread_create(&thdDrain, NULL, drainFifoThread, (void *) targ);
        if (status != 0) {
            if (writeToFile) fprintf(fp, "error creating drain thread\n");
            perror("error creating drain thread");
            return (1);
        }
    }

    ////////////////////////////
    /// Control Plane  Stuff ///
    ////////////////////////////

//    LoadBalancerServiceImpl service;
//    LoadBalancerServiceImpl *pGrpcService = &service;

    // Create grpc client of control plane
    LbControlPlaneClient client(cpAddr, cpPort,
                                listeningAddr, port, pRange,
                                clientName, adminToken, lbid,
                                weight, setPoint);

    // Register this client with the grpc server
    int32_t err = client.Register();
    if (err == -1) {
        fprintf(fp, "GRPC client %s is already registered!\n", clientName);
    }
    else if (err == 1) {
        if (writeToFile) fprintf(fp, "GRPC client %s communication error with server when registering, exit!\n", clientName);
        perror("GRPC client communication error with server when registering");
        return(1);
    }

    // Write header to data file
    if (writeToCsvFile) {
        fprintf(csvFp,
                "timestamp,fifo_mean_fill_pct,fifo_mean_fill,pid_control_variable,events_reassembled,events_processed\n");
    }

    // Add stuff to prevent anti-aliasing.
    // If sampling fifo level every N millisec but that level is changing much more quickly,
    // the sent value will NOT be an accurate representation. It will include a lot of noise.
    // To prevent this, keep a running average of the fill %, so its reported value is a more
    // accurate portrayal of what's really going on.

    // Find # of loops (samples) to comprise one reporting period.
    // Command line enforces report time to be integer multiple of sampleTime.
    int adjustedSampleTime = sampleTime;
    int loopMax   = 1000 * reportTime / sampleTime; // report in millisec, sample in microsec
    int loopCount = loopMax;    // use to track # loops made
    float pidError = 0.F;
    // Keep fcount sample times worth (1 sec) of errors so we can use error from 1 sec ago
    // for PID derivative term. For now, fill w/ 0's.
    float oldestPidError, oldPidErrors[fcount];
    memset(oldPidErrors, 0, fcount*sizeof(float));

    fprintf(fp, "reportTime = %u msec, sampleTime = %u microsec, loopMax = %d, loopCount = %d\n", reportTime, sampleTime, loopMax, loopCount);

    // Keep a running avg of fifo fill over fcount samples
    float runningFillTotal = 0., fillAvg;
    float fillValues[fcount];
    memset(fillValues, 0, fcount*sizeof(float));
    // Keep circulating thru array. Highest index is fcount - 1.
    float prevFill, curFill, fillPercent;


    // Alternatively keep a running avg of the incoming event rate normalized to max event processing rate
    int64_t runningEventTotal = 0;
    int64_t evCountValues[fcount];
    memset(evCountValues, 0, fcount*sizeof(int64_t));
    int64_t prevEvCount, curEvCount;
    // set first and last indexes right here
    int currentIndex = 0, earliestIndex = 1;
    float evRateAvg, relEvRate = 0.F;   // Incoming event rate / max EPR = relative event rate

    // time stuff
    struct timespec t1, t2;
    int64_t totalTime, time; // microsecs
    int64_t totalTimeGoal = sampleTime * fcount;
    int64_t times[fcount];
    float deltaT; // "time" in secs
    int64_t absTime, prevAbsTime, prevFifoTime;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    prevFifoTime = prevAbsTime = 1000000L*(t1.tv_sec) + (t1.tv_nsec)/1000L; // microsec epoch time
    // Load times with current time for more accurate first round of rates
    for (int i=0; i < fcount; i++) {
        times[i] = prevAbsTime;
    }


    while (true) {

        // Delay between sampling fifo points.
        // sampleTime is adjusted below to get close to the actual desired sampling rate.
        // Not sure how well this will work as minimum sleep time on linux is around
        // 1 to 1/2 millisec. That means sampleTime >= 500.
        std::this_thread::sleep_for(std::chrono::microseconds(adjustedSampleTime));

        // Read time
        clock_gettime(CLOCK_MONOTONIC, &t2);
        // time diff in microsec
        time = (1000000L * (t2.tv_sec - t1.tv_sec)) + ((t2.tv_nsec - t1.tv_nsec)/1000L);
        // convert to sec
        deltaT = static_cast<float>(time)/1000000.F;
        // Get the current epoch time in microsec
        absTime = 1000000L*t2.tv_sec + t2.tv_nsec/1000L;
        t1 = t2;


        // Keep count of total time taken for last fcount periods.

        // Record current time
        times[currentIndex] = absTime;
        // Subtract from that the earliest time to get the total time in microsec
        totalTime = absTime - times[earliestIndex];
        // Keep things from blowing up if we've goofed somewhere
        if (totalTime < totalTimeGoal) totalTime = totalTimeGoal;
        // Get oldest pid error for calculating PID derivative term
        oldestPidError = oldPidErrors[earliestIndex];


        // PID error
        if (usePidEpr) {
            // Error term is based on incoming-evt-rate/EPR.

            // Keep a running total on # of events to arrive in fcount periods
            curEvCount  = totalEvents;
            prevEvCount = evCountValues[currentIndex];
            evCountValues[currentIndex] = curEvCount;
            runningEventTotal = curEvCount - prevEvCount;

            // Calculate avg rate over fcount periods and also do the normalization
            evRateAvg  = 1000000.0 * runningEventTotal / totalTime;
            relEvRate  = evRateAvg / maxEPR;
            pidError   = pid<float>(setPoint, relEvRate, deltaT, Kp, Ki, Kd, oldestPidError, totalTime);
        }
        else {
            // Error term based on fifo level.

            // Read current fifo level
            curFill = sharedQ->size();
            // Previous value at this index
            prevFill = fillValues[currentIndex];
            // Store current val at this index
            fillValues[currentIndex] = curFill;
            // Add current val and remove previous val at this index from the running total.
            // That way we have added fcount number of most recent entries at ony one time.
            runningFillTotal += curFill - prevFill;

            // Under crazy circumstances, runningFillTotal could be < 0 !
            // Would have to have high fill, then IMMEDIATELY drop to 0 for about a second.
            // This would happen at very small input rate as otherwise it takes too much
            // time for events in q to be processed and q level won't drop as quickly
            // as necessary to see this effect.
            // If this happens, set runningFillTotal to 0 as the best approximation.
            if (runningFillTotal < 0.) {
                fprintf(fp, "\nNEG runningFillTotal (%f), set to 0!!\n\n", runningFillTotal);
                runningFillTotal = 0.;
            }

            fillAvg = runningFillTotal / fcountFlt;
            fillPercent = fillAvg / fifoCapacityFlt;
            pidError = pid<float>(setPoint, fillPercent, deltaT, Kp, Ki, Kd, oldestPidError, totalTime);
        }


        // Track pid error
        oldPidErrors[currentIndex] = pidError;

        // Set indexes for next round
        earliestIndex++;
        earliestIndex = (earliestIndex == fcount) ? 0 : earliestIndex;

        currentIndex++;
        currentIndex = (currentIndex == fcount) ? 0 : currentIndex;

        if (currentIndex == 0) {
            // Use totalTime to adjust the effective sampleTime so that we really do sample
            // at the desired rate set on command line. This accounts for all the computations
            // that this code does which slows down the actual sample rate.
            // Do this adjustment once every fcount samples.
            float factr = totalTimeGoal/totalTime;
            adjustedSampleTime = sampleTime * factr;

            // If totalTime, for some reason, is really big, we don't want the adjusted time to be 0
            // since a sleep_for(0) is very short. However, sleep_for(1) is pretty much the same as
            // sleep_for(500).
            if (adjustedSampleTime == 0) {
                adjustedSampleTime = 500;
            }

            //fprintf(fp, "sampleTime = %d, totalT = %.0f\n", adjustedSampleTime, totalTime);
        }


        // Every "loopMax" loops
        if (--loopCount <= 0) {
            // Update the changing variables
            if (fixedFill) {
                // test CP by fixing fill level and setting pid error to 0
                client.update(setFill, 0);
            }
            else if (usePidEpr) {
                client.update(relEvRate, pidError);
            }
            else {
                client.update(fillPercent, pidError);
            }

            // Send to server
            err = client.SendState();
            if (err == 1) {
                fprintf(fp, "GRPC client %s communication error with server during sending of data!\n", clientName);
                break;
            }

            if (writeToCsvFile) {
                auto now = std::chrono::system_clock::now();
                std::time_t now_c = std::chrono::system_clock::to_time_t(now);
                char timestamp[20];
                strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", std::localtime(&now_c));
                if (usePidEpr) {
                    fprintf(
                            csvFp,
                            "%s,%f,%f,%f,%d,%d\n",
                            timestamp,
                            relEvRate,
                            evRateAvg,
                            pidError,
                            (int) eventsProcessed,
                            (int) eventsReassembled
                    );
                }
                else {
                    fprintf(
                            csvFp,
                            "%s,%f,%f,%f,%d,%d\n",
                            timestamp,
                            fillPercent,
                            fillAvg,
                            pidError,
                            (int) eventsProcessed,
                            (int) eventsReassembled
                    );
                }
                fflush(csvFp);
            }

            fflush(fp);
            loopCount = loopMax;
        }

        // Print out every 4 seconds
        if (absTime - prevAbsTime >= 4000000) {
            prevAbsTime = absTime;
            if (usePidEpr) {
                fprintf(fp, "     Rel ev rate = %.3f,  Avg in ev rate = %.2f Hz,  pid err %f,  ev asmb = %d,  ev proc = %d\n\n",
                        relEvRate, evRateAvg, pidError, (int) eventsReassembled, (int) eventsProcessed);

            }
            else {
                fprintf(fp, "     Fifo level %d  Avg:  %.2f,  %.2f%%,  pid err %f\n\n",
                        ((int) curFill), fillAvg, (100.F * fillPercent), pidError);
            }
            fflush(fp);
        }
    }

    // Unregister this client with the grpc server
    err = client.Deregister();
    if (err == 1) {
        if (writeToFile) fprintf(fp, "GRPC client %s communication error with server when deregistering, exit!\n", clientName);
        perror("GRPC client communication error with server when deregistering");
        return(1);
    }

    fprintf(fp, "GRPC client %s deregistered\n", clientName);
    return(0);
}
