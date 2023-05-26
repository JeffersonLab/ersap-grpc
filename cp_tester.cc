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
 * Send simulated requests/data to the control_plane_server program.
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

//-----------------------------------------------------------------------
// Be sure to print to stderr as this program pipes data to stdout!!!
//-----------------------------------------------------------------------

/**
 * Print out help.
 * @param programName name to use for this program.
 */
static void printHelp(char *programName) {
    fprintf(stderr,
            "\nusage: %s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n\n",
            programName,
            "        [-h] [-v]",
            "        [-p <data receiving port (for registration)>]",
            "        [-a <data receiving address (for registration)>]",
            "        [-range <data receiving port range (for registration)>]",
            "        [-cp_addr <control plane IP address>]",
            "        [-cp_port <control plane port>]",
            "        [-name <backend name>]",
            "        [-id <backend id#>]",
            "        [-inrate <fill fifo Hz>]",
            "        [-outrate <drain fifo Hz>]",
            "        [-s <PID fifo set point>]");

    fprintf(stderr, "        This is a gRPC program that simulates an ERSAP backend by sending messages to a simulated control plane.\n");
    fprintf(stderr, "        The -p, -a, and -range args are only to tell CP where to send our data, but are unused in this program.\n");
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
 * @param inrate        filled with mean fifo fill rate in Hz.
 * @param outrate       filled with mean ifo drain rate in Hz.
 * @param debug         filled with debug flag.
 * @param cpAddr        filled with grpc server (control plane) IP address to info to.
 * @param clientName    filled with name of this grpc client (backend) to send to control plane.
 */
static void parseArgs(int argc, char **argv,
                      uint32_t *clientId, float *setPt, uint16_t *cpPort,
                      uint16_t *port, int *range, char *listenAddr,
                      uint32_t *inrate, uint32_t *outrate,
                      bool *debug, char *cpAddr, char *clientName) {

    int c, i_tmp;
    bool help = false;
    float sp = 0.;

    /* 4 multiple character command-line options */
    static struct option long_options[] =
            {             {"cp_addr",  1, NULL, 4},
                          {"cp_port",  1, NULL, 5},
                          {"name",  1, NULL, 6},
                          {"id",  1, NULL, 7},
                          {"range",  1, NULL, 8},
                          {"inrate",  1, NULL, 1},
                          {"outrate",  1, NULL, 2},
                          {0,       0, 0,    0}
            };


    while ((c = getopt_long_only(argc, argv, "vhs:", long_options, 0)) != EOF) {

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

            case 1:
                // mean fill fifo rate
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 0 && i_tmp < 100001) {
                    *inrate = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -inrate, 1 < rate <= 100k\n\n");
                    printHelp(argv[0]);
                    exit(-1);
                }
                break;

            case 2:
                // mean drain fifo rate
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 0 && i_tmp < 100001) {
                    *outrate = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -outrate, 1 < rate <= 100k\n\n");
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
    int  avgInsertionRate; // Hz
    int  avgDrainRate;     // Hz
    int  maxFifoLevel;     // 4000 default
//    int  bytesPerFifoEntry;    // 100kB default
    bool debug;
} threadArg;


/**
 * This thread fills the fifo independently of the thread to
 * empty it and the thread to read it and inform control plane.
 *
 * @param arg struct to be passed to thread.
 */
static void *fillFifoThread(void *arg) {

    threadArg *tArg = (threadArg *) arg;

    int fillRate = tArg->avgInsertionRate;
    int maxLevel = tArg->maxFifoLevel;
    bool debug   = tArg->debug;

    // random device class instance, source of 'true' randomness for initializing random seed
    std::random_device rd;
    // Mersenne twister PRNG, initialized with seed from previous random device instance
    std::mt19937 gen(rd());

    // The reassembly thread contructs one buffer at a time and inserts it into the fifo.
    // Thus, to model that, the idea here is to generate a distribution of times between
    // single insertions. So, a little example.
    // For 1GB/s (8Gps) incoming data rate and 62.5kB/event, we get
    // 1e9 / 62.5e3 = 16000 buffers/sec. This translates to a delay of 1/16000 = 62.5 microsec.
    // Here, delay = 1/fillRate in seconds.
    // Thus, we'll want a distribution centered around (1/fillRate)*1e6 microseconds.
    // Delay needs to be an integer. We'll use a Gaussian distribution.
    int meanDelay = 1000000/fillRate;
    // Set standard deviation, say 5%
    int stdDev = meanDelay / 20;
    int microsecDelay;
    uint64_t droppedEvents = 0;

    // Even distribution between 0 & 1
    // std::uniform_real_distribution<> d(0.0, 1.0);

    // Gaussian, mean = avg delay, std dev = 5% of mean
    std::normal_distribution<int> g(meanDelay, stdDev);

    while (true) {
        // Grab mutex
        std::unique_lock<std::mutex> ul(fifoMutex);

        // Wait until fifo is not full
        while (fifoLevel >= maxLevel) {
            fifoCV.wait(ul);
        }

        // Insert buffer into fifo
        fifoLevel++;

        // Unlock mutex
        ul.unlock();

        // Tell drain, there is something to pull off
        fifoCV.notify_one();

        // Get random # in Gaussian dist
        microsecDelay = g(gen);

        // Delay between insertions
        std::this_thread::sleep_for(std::chrono::microseconds(microsecDelay));
    }

    return nullptr;
}



/**
 * This thread drains the fifo independently of the thread to
 * fill it and the thread to read it and inform control plane.
 *
 * @param arg struct to be passed to thread.
 */
static void *drainFifoThread(void *arg) {

    threadArg *tArg = (threadArg *) arg;

    int drainRate = tArg->avgDrainRate;
    int maxLevel  = tArg->maxFifoLevel;
    bool debug    = tArg->debug;

    // random device class instance, source of 'true' randomness for initializing random seed
    std::random_device rd;
    // Mersenne twister PRNG, initialized with seed from previous random device instance
    std::mt19937 gen(rd());

    // The fill thread inserts one item at a time into the fifo.
    // It generates a distribution of times between these single insertions.
    //
    // This thread, on the other hand, pulls out one item at a time, much as would
    // an application that processes one event at a time.
    // As in the insertion thread, we'll use a Gaussian distribution of delay times
    // between each removal, centered around the time corresponding to the given drain rate.
    // Thus, we'll want a distribution centered around (1/drainRate)*1e6 microseconds.
    // Delay needs to be an integer.
    int meanDelay = 1000000/drainRate;
    // Set standard deviation to say 15%. This is most likely significantly larger than
    // the std dev of the filling operation since event processing time can vary widely.
    int stdDev = meanDelay / 6.7;
    int microsecDelay;
    uint64_t emptyFifo = 0;

    // Even distribution between 0 & 1
    // std::uniform_real_distribution<> d(0.0, 1.0);

    // Gaussian, mean = avg delay, std dev = 5% of mean
    std::normal_distribution<int> g(meanDelay, stdDev);

    while (true) {
        // Grab mutex
        std::unique_lock<std::mutex> ul(fifoMutex);

        // Wait until there is data in fifo
        while (fifoLevel <= 0) {
            fifoCV.wait(ul);
        }

        // Remove buffer from fifo
        fifoLevel--;

        // Give up mutex
        ul.unlock();

        // Notify the insert thread that fifo level dropped so there is room for more
        fifoCV.notify_one();

        // Get random # in Gaussian dist
        microsecDelay = g(gen);

        // Delay between removals to simulate event processing
        std::this_thread::sleep_for(std::chrono::microseconds(microsecDelay));
    }

    return nullptr;
}



int main(int argc, char **argv) {

    int udpSocket;
    ssize_t nBytes;

    // Set this to max expected data size
    uint32_t clientId = 0;

    float pidError = 0.F;
    float setPoint = 0.5F;   // set fifo to 1/2 full by default

    uint16_t cpPort = 56789;
    bool debug = false;

    int range;
    uint16_t port = 7777;

    uint32_t inRate, outRate;

    char cpAddr[16];
    memset(cpAddr, 0, 16);
    strcpy(cpAddr, "172.19.22.15"); // ejfat-4 by default

    char clientName[31];
    memset(clientName, 0, 31);

    char listeningAddr[16];
    memset(listeningAddr, 0, 16);

    parseArgs(argc, argv, &clientId, &setPoint, &cpPort, &port, &range,
              listeningAddr, &inRate, & outRate, &debug, cpAddr,  clientName);

    // give it a default name
    if (strlen(clientName) < 1) {
        std::string name = "backend" + std::to_string(clientId);
        std::strcpy(clientName, name.c_str());
    }

    // convert integer range in PortRange enum-
    auto pRange = PortRange(range);

    ///////////////////////////////////
    /// Start Fill & Drain Threads ///
    //////////////////////////////////

    threadArg *targ = (threadArg *) calloc(1, sizeof(threadArg));
    if (targ == nullptr) {
        fprintf(stderr, "out of mem\n");
        return -1;
    }

    targ->avgInsertionRate = inRate; // Hz
    targ->avgDrainRate = outRate;    // Hz
    targ->maxFifoLevel = 3;          // 4000 default
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

    // Fifo
    fifoLevel = 0;
    float fillPercent;
    // The ET system (which acts as a fifo) has fifoLevelMax of eventSize bytes each.
    // This is reported to the control plane during registration.
    int eventSize = 100000;
    int fifoLevelMax = 1000;

    // Reading fifo level
    int loopMax   = 1000;
    int loopCount = loopMax;  // loopMax loops of waitMicroSecs microseconds
    int waitMicroSecs = 1000; // By default, loop every millisec

    // Create grpc client of control plane
    LbControlPlaneClient client(cpAddr, cpPort,
                                listeningAddr, port, pRange,
                                clientName, eventSize, fifoLevelMax, setPoint);

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
    float runningFillTotal = 0., fillAvg;
    float fillValues[loopMax];
    memset(fillValues, 0, loopMax*sizeof(float));

    // Keep circulating thru array. Highest index is loopMax - 1.
    // The first time thru, we don't want to over-weight with (loopMax - 1) zero entries.
    // So we read loopMax entries first, before we start keeping stats & reporting level.
    float prevFill;
    bool startingUp = true;
    int fillIndex = 0, firstLoopCounter = 1;

    while (true) {

        // Delay between data points
        std::this_thread::sleep_for(std::chrono::microseconds(waitMicroSecs));

        // Read current fifo level
        fillPercent = fifoLevel;
        // Previous value at this index
        prevFill = fillValues[fillIndex];
        // Store current val at this index
        fillValues[fillIndex++] = fillPercent;
        // Add current val and remove previous val at this index from the running total.
        // That way we have added loopMax number of most recent entries at ony one time.
        runningFillTotal += fillPercent - prevFill;
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

            printf("Total cnt %d, %f%% filled, %f avg, error %f\n", fifoLevelMax, fillPercent, fillAvg, pidError);

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
