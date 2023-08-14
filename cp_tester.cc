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
            "\nusage: %s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n\n",
            programName,
            "        [-h] [-v] [-ipv6]",
            "        [-p <data receiving port (for registration, 17750 default)>]",
            "        [-a <data receiving address (for registration)>]",
            "        [-range <data receiving port range (for registration)>]",
            "        [-token <authentication token (for registration, default = udplbd_default_change_me)>]",
            "        [-file <fileName to hold output>]\n",

            "        [-cp_addr <control plane IP address (default ejfat-2)>]",
            "        [-cp_port <control plane grpc port (default 18347)>]",
            "        [-name <backend name>]\n",

            "        [-kp <PID proportional constant, default 0.8>]",
            "        [-ki <PID integral constant, default 0.02>]",
            "        [-kd <PID differential constant, default 0.001>]\n",

            "        [-count <# of most recent fill values averaged, default 1000>]",
            "        [-rtime <millisec for reporting fill to CP, default 1 sec>]\n",

            "        [-b <internal buf size to hold event (150kB default)>]",
            "        [-fifo <fifo size (1000 default)>]",
            "        [-s <PID fifo set point (0 default)>]",
            "        [-fill <set reported fifo fill %, 0-1 (and pid error to 0) for testing>]\n");

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
 * @param setPt         filled with the set point of PID loop used with fifo fill level.
 * @param cpPort        filled with grpc server (control plane) port to info to.
 * @param port          filled with UDP receiving data port to listen on.
 * @param range         filled with range of ports in powers of 2 (entropy).
 * @param listenAddr    filled with IP address to listen on for LB data.
 * @param token         filled with authenication token of backend for CP.
 * @param filename      filled with name of file to hold program output instead of stdout.
 * @param bufSize       filled with byte size of internal bufs to hold incoming events.
 * @param fifoSize      filled with max fifo size.
 * @param fillCount     filled with # of fill level measurements to average together before sending.
 * @param reportTime    filled with millisec between reports to CP.
 * @param debug         filled with debug flag.
 * @param useIPv6       filled with use IP version 6 flag.
 * @param cpAddr        filled with grpc server (control plane) IP address to info to.
 * @param clientName    filled with name of this grpc client (backend) to send to control plane.
 * @param kp            filled with PID proportional constant.
 * @param ki            filled with PID integral constant.
 * @param kd            filled with PID differential constant.
 * @param fill          filled with fixed value to report as fifo fill level (0-1).
 */
static void parseArgs(int argc, char **argv,
                      float *setPt, uint16_t *cpPort,
                      uint16_t *port, int *range,
                      char *listenAddr, char *token, char *fileName,
                      uint32_t *bufSize, uint32_t *fifoSize,
                      uint32_t *fillCount, uint32_t *reportTime,
                      bool *debug, bool *useIPv6, char *cpAddr, char *clientName,
                      float *kp, float *ki, float *kd, float *fill) {

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
                          {"range",    1, nullptr, 8},
                          {"token",    1, nullptr, 9},
                          {"file",     1, nullptr, 10},
                          {"kp",       1, nullptr, 11},
                          {"ki",       1, nullptr, 12},
                          {"kd",       1, nullptr, 13},
                          {"count",    1, nullptr, 14},
                          {"rtime",    1, nullptr, 15},
                          {"fill",     1, nullptr, 16},
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
                // Set the Kp PID loop parameter
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -kp\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                *kp = sp;
                break;

            case 12:
                // Set the Ki PID loop parameter
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -ki\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                *ki = sp;
                break;

            case 13:
                // Set the Kd PID loop parameter
                try {
                    sp = (float) std::stof(optarg, nullptr);
                }
                catch (const std::invalid_argument& ia) {
                    fprintf(stderr, "Invalid argument to -kd\n\n");
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


// Statistics
static volatile uint64_t totalBytes=0, totalPackets=0, totalEvents=0;
//static std::atomic<uint32_t> droppedPackets {0};
//static std::atomic<uint32_t> droppedEvents {0};
//static std::atomic<uint32_t> droppedBytes {0};
static uint32_t droppedPackets=0, droppedEvents=0, droppedBytes=0;



// Arg to pass to fifo fill/drain threads
typedef struct threadArg_t {
    // Statistics
    std::shared_ptr<ejfat::packetRecvStats> stats;
    std::shared_ptr<ejfat::queue<std::vector<char>>> sharedQ;
    int  udpSocket;
    uint32_t bufSize;
    bool debug;
    bool writeToFile;
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
    FILE *fp         = tArg->fp;

    uint32_t tickPrescale = 1;
    uint64_t tick;
    uint16_t dataId;
    ssize_t  nBytes;

    clearStats(stats);
    uint32_t drPkts = 0, prevDrPkts = 0;

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

        // Fill vector with data. Insert data about packet order.
        nBytes = getReassembledBuffer(vec, udpSocket, debug, &tick, &dataId, stats, tickPrescale);
        if (nBytes < 0) {
            if (writeToFile) fprintf(fp, "Error in getReassembledBuffer, %ld\n", nBytes);
            perror("Error in getReassembledBuffer");
            exit(1);
        }

        // Receiving stats

        totalBytes += nBytes;
        // stats keeps a running total in which getReassembledBuffer adds to it with each call since
        // stats is never cleared between calls
        totalPackets = stats->acceptedPackets;
        totalEvents++;

        // atomic
        droppedBytes   = stats->discardedBytes;
        droppedEvents  = stats->discardedBuffers;
        droppedPackets = stats->discardedPackets;

        drPkts = stats->droppedPackets;
        if (drPkts > prevDrPkts) {
            printf("dropped = %u, tp = %" PRIu64 "\n", drPkts, totalPackets);
            prevDrPkts = drPkts;
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

    auto sharedQ  = tArg->sharedQ;
    bool debug    = tArg->debug;
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
            fprintf(fp, "Pkt delay %u usec, %u total pkts, arrival sequence:\n", delay, totalPkts);

            uint32_t seq;
            for (int i=0; i < totalPkts; i++) {
                seq = buf[12 + 4*i];
                fprintf(fp, " %u", seq);
            }
            fprintf(fp, "\n");
        }

        // Delay to simulate data processing
        std::this_thread::sleep_for(std::chrono::microseconds(delay));
    }

    return nullptr;
}


// Thread to send to print out rates
static void *rateThread(void *arg) {

    uint64_t packetCount, byteCount, eventCount;
    uint64_t prevTotalPackets, prevTotalBytes, prevTotalEvents;
    uint64_t currTotalPackets, currTotalBytes, currTotalEvents;

    uint64_t dropPacketCount, dropByteCount, dropEventCount;
    uint64_t currDropTotalPackets, currDropTotalBytes, currDropTotalEvents;
    uint64_t prevDropTotalPackets, prevDropTotalBytes, prevDropTotalEvents;

    // Ignore first rate calculation as it's most likely a bad value
    bool skipFirst = true;

    double pktRate, pktAvgRate, dataRate, dataAvgRate, totalRate, totalAvgRate, evRate, avgEvRate;
    int64_t totalT = 0, time, droppedPkts, totalDroppedPkts = 0, droppedEvts, totalDroppedEvts = 0;
    uint64_t absTime;
    struct timespec t1, t2, firstT;

    // Get the current time
    clock_gettime(CLOCK_MONOTONIC, &t1);
    firstT = t1;

    while (true) {

        prevTotalBytes   = totalBytes;
        prevTotalPackets = totalPackets;
        prevTotalEvents  = totalEvents;

        prevDropTotalBytes   = droppedEvents;
        prevDropTotalPackets = droppedPackets;
        prevDropTotalEvents  = droppedEvents;

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

        currDropTotalBytes   = droppedEvents;
        currDropTotalPackets = droppedPackets;
        currDropTotalEvents  = droppedEvents;

        if (skipFirst) {
            // Don't calculate rates until data is coming in
            if (currTotalPackets > 0) {
                skipFirst = false;
            }
            firstT = t1 = t2;
            totalT = totalBytes = totalPackets = totalEvents = 0;
            droppedBytes = droppedPackets = droppedEvents = 0;
            continue;
        }

        // Use for instantaneous rates
        byteCount   = currTotalBytes   - prevTotalBytes;
        packetCount = currTotalPackets - prevTotalPackets;
        eventCount  = currTotalEvents  - prevTotalEvents;

        dropByteCount   = currDropTotalBytes   - prevDropTotalBytes;
        dropPacketCount = currDropTotalPackets - prevDropTotalPackets;
        dropEventCount  = currDropTotalEvents  - prevDropTotalEvents;

        // Reset things if #s rolling over
        if ( (byteCount < 0) || (totalT < 0) )  {
            totalT = totalBytes = totalPackets = totalEvents = 0;
            droppedBytes = droppedPackets = droppedEvents = 0;
            firstT = t1 = t2;
            continue;
        }

        pktRate = 1000000.0 * ((double) packetCount) / time;
        pktAvgRate = 1000000.0 * ((double) currTotalPackets) / totalT;
//        printf("Packets:       %3.4g Hz,    %3.4g Avg, time: diff = %" PRId64 " usec, abs = %" PRIu64 " epoch msec\n",
//                pktRate, pktAvgRate, time, absTime);

        printf("Packets:       %3.4g Hz,    %3.4g Avg, total %" PRIu64 "\n",
                pktRate, pktAvgRate, currTotalPackets);

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
        printf("Events:        %3.4g Hz,  %3.4g Avg, total %" PRIu64 "\n", evRate, avgEvRate, totalEvents);

        // Drop info
        printf("Dropped: evts: %" PRIu64 ", %" PRIu64 " total, pkts: %" PRIu64 ", %" PRIu64 " total\n\n",
                dropEventCount, currDropTotalEvents, dropPacketCount, currDropTotalPackets);

        t1 = t2;
    }

    return (nullptr);
}


int main(int argc, char **argv) {

    ssize_t nBytes;
    uint32_t bufSize = 150000; // 150kB default

    float setFill  = -1.0F;
    float pidError = 0.F;
    float setPoint = 0.F;   // set fifo to 1/2 full by default

    uint16_t cpPort = 18347;
    bool debug = false;
    bool useIPv6 = false;
    bool writeToFile = false;
    bool fixedFill = false;

    int range;
    uint16_t port = 17750;

    uint32_t fifoCapacity = 1000;

    // PID loop variables
    float Kp = 0.8;
    float Ki = 0.02;
    float Kd = 0.00;

    // # of fill values to average when reporting to grpc
    uint32_t fcount = 1000;
    // time period in millisec for reporting to CP
    uint32_t reportTime = 1000;

    char cpAddr[16];
    memset(cpAddr, 0, 16);
    strcpy(cpAddr, "129.57.177.144"); // ejfat-2 by default

    char clientName[31];
    memset(clientName, 0, 31);

    char listeningAddr[16];
    memset(listeningAddr, 0, 16);

    char fileName[128];
    memset(fileName, 0, 128);

    char authToken[256];
    memset(authToken, 0, 256);
    strcpy(authToken, "udplbd_default_change_me");

    parseArgs(argc, argv, &setPoint, &cpPort, &port, &range,
              listeningAddr, authToken, fileName,
              &bufSize, &fifoCapacity, &fcount, &reportTime,
              &debug, &useIPv6, cpAddr,  clientName, &Kp, &Ki, &Kd, &setFill);

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

    ///////////////////////////////////
    ///       output to file        ///
    ///////////////////////////////////

    // By default output goes to stderr
    FILE *fp = stderr;

    // else send to file
    if (strlen(fileName) > 0) {
        fp = fopen(fileName, "w");
        if (!fp) {
            fprintf(stderr, "file open failed: %s\n", strerror(errno));
            return(1);
        }
        writeToFile = true;
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
    /// Start Fill & Drain Threads ///
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
    targ->fp = fp;

    pthread_t thdFill;
    status = pthread_create(&thdFill, NULL, fillFifoThread, (void *) targ);
    if (status != 0) {
        if (writeToFile) fprintf(fp, "error creating fill thread\n");
        perror("error creating fill thread");
        return(1);
    }

    pthread_t thdDrain;
    status = pthread_create(&thdDrain, NULL, drainFifoThread, (void *) targ);
    if (status != 0) {
        if (writeToFile) fprintf(fp, "error creating drain thread\n");
        perror("error creating drain thread");
        return(1);
    }

    ////////////////////////////
    /// Control Plane  Stuff ///
    ////////////////////////////

    LoadBalancerServiceImpl service;
    LoadBalancerServiceImpl *pGrpcService = &service;

    // Create grpc client of control plane
    LbControlPlaneClient client(cpAddr, cpPort,
                                listeningAddr, port, pRange,
                                clientName, authToken,
                                bufSize, fifoCapacity, setPoint);

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

    // Add stuff to prevent anti-aliasing.
    // If sampling fifo level every millisec but that level is changing much more quickly,
    // the sent value will NOT be an accurate representation. It will include a lot of noise.
    // To prevent this,
    // keep a running average of the fill %, so its reported value is a more accurate portrayal
    // of what's really going on. In this case a running avg is taken over the reporting time.

    float deltaT = (1.0/1000.0); // 1 millisec in seconds
    int sampleMicroSecs = 1000; // Sample data every 1 millisec
    // # of loops (samples) to comprise one reporting period =
    int loopMax   = 1000 * reportTime / sampleMicroSecs; // remember, report time is in millisec
    int loopCount = loopMax;    // use to track # loops made


    float runningFillTotal = 0., fillAvg;
    float fillValues[fcount];
    memset(fillValues, 0, fcount*sizeof(float));

    // Keep circulating thru array. Highest index is fcount - 1.
    // The first time thru, we don't want to over-weight with (fcount - 1) zero entries.
    // So we read fcount entries first, before we start keeping stats & reporting level.
    float prevFill, curFill, fillPercent;
    bool startingUp = true;
    int fillIndex = 0, firstLoopCounter = 1;

    // time stuff
    struct timespec t1, t2;
    int64_t time;
    uint64_t absTime, prevAbsTime;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    prevAbsTime = 1000L*(t1.tv_sec) + (t1.tv_nsec)/1000000L;


    while (true) {

        // Delay between data points
        std::this_thread::sleep_for(std::chrono::microseconds(sampleMicroSecs));

        // Read current fifo level
        curFill = sharedQ->size();
        // Previous value at this index
        prevFill = fillValues[fillIndex];
        // Store current val at this index
        fillValues[fillIndex++] = curFill;
        // Add current val and remove previous val at this index from the running total.
        // That way we have added fcount number of most recent entries at ony one time.
        runningFillTotal += curFill - prevFill;
        // Find index for the next round
        fillIndex = (fillIndex == fcount) ? 0 : fillIndex;

        if (startingUp) {
            if (firstLoopCounter++ >= fcount) {
                // Done with first fcount loops
                startingUp = false;
            }
            else {
                if (firstLoopCounter == fcount) {
                    // Start the clock NOW
                    clock_gettime(CLOCK_MONOTONIC, &t1);
                }
                // Don't start sending data or recording values
                // until the startup time (fcount loops) is over.
                continue;
            }
            fillAvg = runningFillTotal / static_cast<float>(fcount);
        }
        else {
            fillAvg = runningFillTotal / static_cast<float>(fcount);
        }

        fillPercent = fillAvg / static_cast<float>(fifoCapacity);

        // Read time
        clock_gettime(CLOCK_MONOTONIC, &t2);
        // time diff in microsec
        time = (1000000L * (t2.tv_sec - t1.tv_sec)) + ((t2.tv_nsec - t1.tv_nsec)/1000L);
        // convert to sec
        deltaT = static_cast<float>(time)/1000000.F;
        // Get the current epoch time in millisec
        absTime = 1000L*t2.tv_sec + t2.tv_nsec/1000000L;
        t1 = t2;

        // PID error
        pidError = pid<float>(setPoint, fillPercent, deltaT, Kp, Ki, Kd);

        // Every "loopMax" loops
        if (--loopCount <= 0) {
            // Update the changing variables
            if (fixedFill) {
                // test CP by fixing fill level and setting pid error to 0
                client.update(setFill, 0);
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

            loopCount = loopMax;
        }

        // Print out every 4 seconds
        if (absTime - prevAbsTime >= 4000) {
            prevAbsTime = absTime;
            fprintf(fp, "     Fifo %d%% filled, %d avg level, pid err %f\n",
                    (int) (fillPercent * 100), (int) fillAvg, pidError);
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
