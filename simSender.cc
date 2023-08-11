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
 * <p>
 * Send test data buffers repeatedly to an ejfat router which then passes it
 * to the simulated backend program cp_tester.cc. This uses the new, version 2,
 * RE header.
 * </p>
 *
 * <p>
 * The buf size can be given on the cmd line.
 * It can be set exactly to that value, or may be a gaussian dist
 * around that value. Same thing with buffer send rate.
 * After the 2 headers (for LB and RE), encoded in the data, for each packet, is:
 * <ol>
 * <li>Number of microseconds to take for the simulated processing
 *     of reassembled buffer on the receiving end, unsigned 4-byte int in network byte order</li>
 * <li>Seq of packet order (1,2,3 ..) for sending a buffer (4 byte int).
 * <li>Total # of packets comprising buf (4 byte int) </li>
 * </ol>
 * </p>
 *
 * <p>
 * Note that certain command line options are incompatible.
 * One cannot set a delay (-d) and also try to set the buffer rate (-bufrate)
 * or byte rate (-byterate).
 *
 * </p>
 */


#include <unistd.h>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include <thread>
#include <pthread.h>
#include <iostream>
#include <cinttypes>
#include <random>

#include "ersap_grpc_packetize.hpp"

#ifdef __linux__
#ifndef _GNU_SOURCE
        #define _GNU_SOURCE
    #endif

    #include <sched.h>
    #include <pthread.h>
#endif


using namespace ejfat;

#define INPUT_LENGTH_MAX 256



static void printHelp(char *programName) {
    fprintf(stderr,
            "\nusage: %s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n\n",
            programName,
            "        [-h] [-v] [-ipv6] [-sync]\n",

            "        [-d <delay in microsec between packets or buffers depending on -bufdelay>]",
            "        [-bufdelay] (delay between each buffer, not packet)",
            "        [-bufrate  <buffers per sec>]",
            "        [-byterate <bytes per sec>]\n",

            "        [-time <microsec for receiver to delay to simulate processing>]",
            "        [-twidth <microsec 1/2 width of gaussian for variable processing delay>]\n",

            "        [-host <destination host (default 127.0.0.1)>]",
            "        [-p <destination UDP port (default 19522)>]\n",

            "        [-cphost <control plane host (defauls 127.0.0.1)>]",
            "        [-cpport <control plane sync msg port (default 19523)>]\n",

            "        [-i <outgoing interface name (e.g. eth0, currently only used to find MTU)>]",
            "        [-mtu <desired MTU size>]",
            "        [-t <tick>]",
            "        [-ver <version>]",
            "        [-id <data id>]",
            "        [-pro <protocol>]",
            "        [-e <entropy>]\n",

            "        [-b <buffer size, 2MB max, 62.5kB default>]",
            "        [-bwidth <byte 1/2 width of gaussian for variable buffer size>]",
            "        [-s <UDP send buffer size>]\n",

            "        [-cores <comma-separated list of cores to run on>]",
            "        [-tpre <tick prescale (1,2, ... tick increment each buffer sent)>]",
            "        [-dpre <delay prescale (1,2, ... if -d defined, 1 delay for every prescale pkts/bufs)>]\n");

    fprintf(stderr, "        EJFAT UDP packet sender that will packetize and send buffer repeatedly and get stats\n");
    fprintf(stderr, "        By default, data is copied into buffer and \"send()\" is used (connect is called).\n");
    fprintf(stderr, "        If specifying twidth or bwidth, backend time and buf size (-time, -b) are mean values and must be > 0\n");
    fprintf(stderr, "        The -sync option will send a UDP message to LB control plane every second with last tick sent.\n");
}



static void parseArgs(int argc, char **argv, int* mtu, int *protocol,
                      int *entropy, int *version,
                      uint16_t *id, uint16_t* port, uint16_t* cpport,
                      uint64_t* tick, uint32_t* delay,
                      uint64_t *bufSize, uint64_t *bufRate,
                      uint64_t *byteRate, uint32_t *sendBufSize,
                      uint32_t *delayPrescale, uint32_t *tickPrescale,
                      uint32_t *time, uint32_t *timeWidth, uint32_t *sizeWidth,
                      int *cores,
                      bool *debug, bool *useIPv6, bool *bufDelay, bool *sendSync,
                      char* host, char* cphost, char *interface) {

    *mtu = 0;
    int c, i_tmp;
    int64_t tmp;
    bool help = false;

    /* multiple character command-line options */
    static struct option long_options[] =
            {{"mtu",      1, NULL, 1},
             {"host",     1, NULL, 2},
             {"ver",      1, NULL, 3},
             {"id",       1, NULL, 4},
             {"pro",      1, NULL, 5},
             {"sync",     0, NULL, 6},
             {"dpre",     1, NULL, 9},
             {"tpre",     1, NULL, 10},
             {"ipv6",     0, NULL, 11},
             {"bufdelay", 0, NULL, 12},
             {"cores",    1, NULL, 13},
             {"bufrate",  1, NULL, 14},
             {"byterate", 1, NULL, 15},
             {"time",     1, NULL, 16},
             {"twidth",   1, NULL, 17},
             {"bwidth",   1, NULL, 18},
             {"cphost",   1, NULL, 19},
             {"cpport",   1, NULL, 20},
             {0,       0, 0,    0}
            };


    while ((c = getopt_long_only(argc, argv, "vhp:i:t:d:b:s:e:", long_options, 0)) != EOF) {

        if (c == -1)
            break;

        switch (c) {

            case 't':
                // TICK
                tmp = strtoll(optarg, nullptr, 0);
                if (tmp > -1) {
                    *tick = tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -t, tick > 0\n");
                    exit(-1);
                }
                break;

            case 'p':
                // PORT
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 1023 && i_tmp < 65535) {
                    *port = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -p, 1023 < port < 65536\n");
                    exit(-1);
                }
                break;

            case 20:
                // control plane PORT
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 1023 && i_tmp < 65535) {
                    *cpport = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -cpport, 1023 < port < 65536\n");
                    exit(-1);
                }
                break;

            case 'b':
                // BUFFER SIZE
                tmp = strtol(optarg, nullptr, 0);
                if (tmp > 2000000) {
                    *bufSize = 2000000;
                }
                else if (tmp >= 1500) {
                    *bufSize = tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -b, buf size >= 1500 and <= 2MB\n");
                    exit(-1);
                }
                break;

            case 's':
                // UDP SEND BUFFER SIZE
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp >= 100000) {
                    *sendBufSize = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -s, UDP send buf size >= 100kB\n");
                    exit(-1);
                }
                break;

            case 'e':
                // ENTROPY
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp < 0) {
                    fprintf(stderr, "Invalid argument to -e. Entropy must be >= 0\n");
                    exit(-1);
                }
                *entropy = i_tmp;
                break;

            case 'd':
                // DELAY
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp > 0) {
                    *delay = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -d, packet delay > 0\n");
                    exit(-1);
                }
                break;

            case 'i':
                // OUTGOING INTERFACE NAME / IP ADDRESS
                if (strlen(optarg) > 15 || strlen(optarg) < 7) {
                    fprintf(stderr, "interface address is bad\n");
                    exit(-1);
                }
                strcpy(interface, optarg);
                break;

            case 1:
                // MTU
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp < 100) {
                    fprintf(stderr, "Invalid argument to -mtu. MTU buffer size must be > 100\n");
                    exit(-1);
                }
                *mtu = i_tmp;
                break;

            case 2:
                // DESTINATION HOST
                if (strlen(optarg) >= INPUT_LENGTH_MAX) {
                    fprintf(stderr, "Invalid argument to -host, host name is too long\n");
                    exit(-1);
                }
                strcpy(host, optarg);
                break;

            case 19:
                // control plane HOST
                if (strlen(optarg) >= INPUT_LENGTH_MAX) {
                    fprintf(stderr, "Invalid argument to -cphost, host name is too long\n");
                    exit(-1);
                }
                strcpy(cphost, optarg);
                break;

            case 3:
                // VERSION
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp < 0 || i_tmp > 31) {
                    fprintf(stderr, "Invalid argument to -ver. Version must be >= 0 and < 32\n");
                    exit(-1);
                }
                *version = i_tmp;
                break;

            case 4:
                // DATA_ID
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp < 0 || i_tmp > 65535) {
                    fprintf(stderr, "Invalid argument to -id. Id must be >= 0 and < 65536\n");
                    exit(-1);
                }
                *id = i_tmp;
                break;

            case 5:
                // PROTOCOL
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp < 0) {
                    fprintf(stderr, "Invalid argument to -pro. Protocol must be >= 0\n");
                    exit(-1);
                }
                *protocol = i_tmp;
                break;

            case 6:
                // do we send sync messages to LB?
                *sendSync = true;
                break;

            case 9:
                // Delay prescale
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp >= 1) {
                    *delayPrescale = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -dpre, dpre >= 1\n");
                    exit(-1);
                }
                break;

            case 10:
                // Tick prescale
                i_tmp = (int) strtol(optarg, nullptr, 0);
                if (i_tmp >= 1) {
                    *tickPrescale = i_tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -tpre, tpre >= 1\n");
                    exit(-1);
                }
                break;

            case 11:
                // use IP version 6
                *useIPv6 = true;
                break;

            case 12:
                // delay is between buffers not packets
                *bufDelay = true;
                break;

            case 13:
                // Cores to run on
                if (strlen(optarg) < 1) {
                    fprintf(stderr, "Invalid argument to -cores, need comma-separated list of core ids\n");
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
                            fprintf(stderr, "Invalid argument to -cores, need comma-separated list of core ids\n");
                            exit(-1);
                        }
                        index++;
                        //std::cout << s << std::endl;
                    }
                }
                break;

            case 14:
                // Buffers to be sent per second
                if (*byteRate > 0) {
                    fprintf(stderr, "Cannot specify bufrate if byterate already specified\n");
                    exit(-1);
                }

                tmp = strtol(optarg, nullptr, 0);
                if (tmp > 0) {
                    *bufRate = tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -bufrate, bufrate > 0\n");
                    exit(-1);
                }
                break;

            case 15:
                // Bytes to be sent per second
                if (*bufRate > 0) {
                    fprintf(stderr, "Cannot specify byterate if bufrate already specified\n");
                    exit(-1);
                }

                tmp = strtol(optarg, nullptr, 0);
                if (tmp > 0) {
                    *byteRate = tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -byterate, byterate > 0\n");
                    exit(-1);
                }
                break;

            case 16:
                // Time in microsec for receiver (simulated backend)
                // to delay for simulated data processing.
                tmp = strtol(optarg, nullptr, 0);
                if (tmp >= 0) {
                    *time = tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -time, time >= 0\n");
                    exit(-1);
                }
                break;

            case 17:
                // Time width - gaussian width of times for receiver to delay,
                // centered on time given by -time option
                tmp = strtol(optarg, nullptr, 0);
                if (tmp >= 0) {
                    *timeWidth = tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -twidth, time width >= 0\n");
                    exit(-1);
                }
                break;

            case 18:
                // Size width - gaussian width of buf size,
                // centered on size given by -b option
                tmp = strtol(optarg, nullptr, 0);
                if (tmp >= 0) {
                    *sizeWidth = tmp;
                }
                else {
                    fprintf(stderr, "Invalid argument to -bwidth, byte width >= 0\n");
                    exit(-1);
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

    // If we specify the byte/buffer send rate, then all delays are removed
    if (*byteRate > 0 || *bufRate) {
        fprintf(stderr, "Byte rate set to %" PRIu64 " bytes/sec, all delays removed!\n", *byteRate);
        *bufDelay = false;
        *delayPrescale = 1;
        *delay = 0;
    }

    if (help) {
        printHelp(argv[0]);
        exit(2);
    }
}


// Statistics
static volatile uint64_t totalBytes=0, totalPackets=0, totalEvents=0;


// Thread to send to print out rates
static void *thread(void *arg) {

    uint64_t packetCount, byteCount, eventCount;
    uint64_t prevTotalPackets, prevTotalBytes, prevTotalEvents;
    uint64_t currTotalPackets, currTotalBytes, currTotalEvents;
    // Ignore first rate calculation as it's most likely a bad value
    bool skipFirst = true;

    double rate, avgRate, totalRate, totalAvgRate, evRate, avgEvRate;
    int64_t totalT = 0, time;
    uint64_t absTime;
    struct timespec t1, t2, firstT;

    // Get the current time
    clock_gettime(CLOCK_MONOTONIC, &t1);
    firstT = t1;

    while (true) {

        prevTotalBytes   = totalBytes;
        prevTotalPackets = totalPackets;
        prevTotalEvents  = totalEvents;

        // Delay 4 seconds between printouts
        std::this_thread::sleep_for(std::chrono::seconds(4));

        // Read epoch time
        clock_gettime(CLOCK_MONOTONIC, &t2);
        // Epoch time in milliseconds
        absTime = 1000L*(t2.tv_sec) + (t2.tv_nsec)/1000000L;
        // time diff in microseconds
        time = (1000000L * (t2.tv_sec - t1.tv_sec)) + ((t2.tv_nsec - t1.tv_nsec)/1000L);
        totalT = (1000000L * (t2.tv_sec - firstT.tv_sec)) + ((t2.tv_nsec - firstT.tv_nsec)/1000L);

        currTotalBytes   = totalBytes;
        currTotalPackets = totalPackets;
        currTotalEvents  = totalEvents;

        if (skipFirst) {
            // Don't calculate rates until data is coming in
            if (currTotalPackets > 0) {
                skipFirst = false;
            }
            firstT = t1 = t2;
            totalT = totalBytes = totalPackets = totalEvents = 0;
            continue;
        }

        // Use for instantaneous rates
        byteCount   = currTotalBytes   - prevTotalBytes;
        packetCount = currTotalPackets - prevTotalPackets;
        eventCount  = currTotalEvents  - prevTotalEvents;

        // Reset things if #s rolling over
        if ( (byteCount < 0) || (totalT < 0) )  {
            totalT = totalBytes = totalPackets = totalEvents = 0;
            firstT = t1 = t2;
            continue;
        }

        // Packet rates
        rate = 1000000.0 * ((double) packetCount) / time;
        avgRate = 1000000.0 * ((double) currTotalPackets) / totalT;
        printf("Packets:       %3.4g Hz,    %3.4g Avg, time: diff = %" PRId64 " usec, abs = %" PRIu64 " epoch msec\n",
                rate, avgRate, time, absTime);

        // Data rates (with NO header info)
        rate = ((double) byteCount) / time;
        avgRate = ((double) currTotalBytes) / totalT;
        // Data rates (with RE header info)
        totalRate = ((double) (byteCount + RE_HEADER_BYTES*packetCount)) / time;
        totalAvgRate = ((double) (currTotalBytes + RE_HEADER_BYTES*currTotalPackets)) / totalT;
        printf("Data (+hdrs):  %3.4g (%3.4g) MB/s,  %3.4g (%3.4g) Avg\n", rate, totalRate, avgRate, totalAvgRate);

        // Event rates
        evRate = 1000000.0 * ((double) eventCount) / time;
        avgEvRate = 1000000.0 * ((double) currTotalEvents) / totalT;
        printf("Events:        %3.4g Hz,  %3.4g Avg, total %" PRIu64 "\n\n", evRate, avgEvRate, totalEvents);

        t1 = t2;
    }

    return (nullptr);
}






/**
 * Doing things this way is like reading a buffer bit-by-bit
 * and passing it off to the parser bit-by-bit
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char **argv) {

    uint32_t beDelayTime = 0, timeWidth = 0, sizeWidth = 0;
    uint32_t tickPrescale = 1;
    uint32_t delayPrescale = 1, delayCounter = 0;
    uint32_t offset = 0, sendBufSize = 0;
    uint32_t delay = 0, packetDelay = 0, bufferDelay = 0;
    uint64_t bufRate = 0L, bufSize = 62500L, byteRate = 0L;
    uint16_t port = 0x4c42, cpport = 0x4c43; // 19522 & 19523
    uint64_t tick = 0;
    int cores[10];
    int mtu, version = 2, protocol = 1, entropy = 0;
    uint16_t dataId = 1;
    bool debug = false;
    bool useIPv6 = false, bufDelay = false;
    bool setBufRate = false, setByteRate = false;
    bool sendSync = false;
    bool useSizeSpread = false, useTimeSpread = false;

    char syncBuf[28];
    char host[INPUT_LENGTH_MAX], cphost[INPUT_LENGTH_MAX], interface[16];
    memset(host, 0, INPUT_LENGTH_MAX);
    memset(cphost, 0, INPUT_LENGTH_MAX);
    memset(interface, 0, 16);
    strcpy(host, "127.0.0.1");
    strcpy(cphost, "127.0.0.1");
    strcpy(interface, "lo0");
    for (int i=0; i < 10; i++) {
        cores[i] = -1;
    }

    parseArgs(argc, argv, &mtu, &protocol, &entropy, &version, &dataId, &port, &cpport, &tick,
              &delay, &bufSize, &bufRate, &byteRate, &sendBufSize,
              &delayPrescale, &tickPrescale, &beDelayTime, &timeWidth, &sizeWidth, cores, &debug,
              &useIPv6, &bufDelay, &sendSync, host, cphost, interface);

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
                std::cerr << "Run sending thread on core " << cores[i] << "\n";
                CPU_SET(cores[i], &cpuset);
            }
            else {
                break;
            }
        }
        pthread_t current_thread = pthread_self();
        int rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }

    std::cerr << "Initially running on cpu " << sched_getcpu() << "\n";

#endif

    fprintf(stderr, "send = %s\n", btoa(send));

    if (bufDelay) {
        // Delay between buffers?
        packetDelay = 0;
        bufferDelay = delay;
    }
    else {
        // Delay between packets?
        packetDelay = delay;
        bufferDelay = 0;
    }

    if (byteRate > 0) {
        // Are we trying to send a fixed byte rate?
        setByteRate = true;
    }
    else if (bufRate > 0) {
        // Are we trying to send a fixed buffer rate?
        setBufRate = true;
    }

    // Do we use gaussian distribution of simulated BE processing times?
    if (timeWidth > 0 && beDelayTime > 0) {
        useTimeSpread = true;
    }

    // Do we use gaussian distribution of buffer sizes?
    if (sizeWidth > 0) {
        useSizeSpread = true;
    }

    // Break data into multiple packets of max MTU size.
    // If the mtu was not set on the command line, get it progamatically
    if (mtu == 0) {
        mtu = getMTU(interface, true);
    }

    // Jumbo (> 1500) ethernet frames are 9000 bytes max.
    // Don't exceed this limit.
    if (mtu > 9000) {
        mtu = 9000;
    }

    fprintf(stderr, "Using MTU = %d\n", mtu);

    // 20 bytes = normal IPv4 packet header (60 is max), 8 bytes = max UDP packet header
    // https://stackoverflow.com/questions/42609561/udp-maximum-packet-size
    int maxUdpPayload = mtu - 20 - 8 - HEADER_BYTES;

    fprintf(stderr, "Setting max UDP payload size to %d bytes, MTU = %d\n", maxUdpPayload, mtu);


    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Create a variable backend processing time (gausssian around the given backend time)
    uint32_t backendTime = beDelayTime;
    fprintf(stderr, "BACKEND TIME = %u, useTimeWidth = %s\n", beDelayTime, btoa(useTimeSpread));

    // For generating random, distributed numbers
    std::random_device rd;
    std::mt19937 gen {rd()};

    // Gaussian dist for times
    std::normal_distribution<float> timeDist {(float)beDelayTime, (float)timeWidth};

    // To use this to generate time:
    // float r = timeDist(gen);


    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Determine size of data to send

    // Create a variable event buffer size (gausssian around the given buf size)
    uint32_t bufByteSize = bufSize;

    // Gaussian dist for buffer sizes
    std::normal_distribution<float> bufDist {(float)bufSize, (float)sizeWidth};

    // To use this to generate size:
    // float r = bufDist(gen);


    ///////////////////////////////////////////////////////////////////////////////////////////////

    // Create UDP sockets (one for control plane, the other for backend)
    int cpSocket, clientSocket;

    // Create socket to backend
    if (useIPv6) {
        struct sockaddr_in6 serverAddr6;

        /* create a DGRAM (UDP) socket in the INET/INET6 protocol */
        if ((clientSocket = socket(AF_INET6, SOCK_DGRAM, 0)) < 0) {
            perror("creating IPv6 client socket");
            return -1;
        }

        socklen_t size = sizeof(int);
        int sendBufBytes = 0;
#ifndef __APPLE__
        // Try to increase send buf size - by default to 25 MB
            sendBufBytes = sendBufSize <= 0 ? 25000000 : sendBufSize;
            setsockopt(clientSocket, SOL_SOCKET, SO_SNDBUF, &sendBufBytes, sizeof(sendBufBytes));
#endif
        sendBufBytes = 0; // clear it
        getsockopt(clientSocket, SOL_SOCKET, SO_SNDBUF, &sendBufBytes, &size);
        fprintf(stderr, "UDP socket send buffer = %d bytes\n", sendBufBytes);

        // Configure settings in address struct
        // Clear it out
        memset(&serverAddr6, 0, sizeof(serverAddr6));
        // it is an INET address
        serverAddr6.sin6_family = AF_INET6;
        // the port we are going to send to, in network byte order
        serverAddr6.sin6_port = htons(port);
        // the server IP address, in network byte order
        inet_pton(AF_INET6, host, &serverAddr6.sin6_addr);

        int err = connect(clientSocket, (const sockaddr *) &serverAddr6, sizeof(struct sockaddr_in6));
        if (err < 0) {
            if (debug) perror("Error connecting UDP socket:");
            close(clientSocket);
            exit(1);
        }
    }
    else {
        struct sockaddr_in serverAddr;

        // Create UDP socket
        if ((clientSocket = socket(PF_INET, SOCK_DGRAM, 0)) < 0) {
            perror("creating IPv4 client socket");
            return -1;
        }

        // Try to increase send buf size to 25 MB
        socklen_t size = sizeof(int);
        int sendBufBytes = 0;
#ifndef __APPLE__
        // Try to increase send buf size - by default to 25 MB
            sendBufBytes = sendBufSize <= 0 ? 25000000 : sendBufSize;
            setsockopt(clientSocket, SOL_SOCKET, SO_SNDBUF, &sendBufBytes, sizeof(sendBufBytes));
#endif
        sendBufBytes = 0; // clear it
        getsockopt(clientSocket, SOL_SOCKET, SO_SNDBUF, &sendBufBytes, &size);
        fprintf(stderr, "UDP socket send buffer = %d bytes\n", sendBufBytes);

        // Configure settings in address struct
        memset(&serverAddr, 0, sizeof(serverAddr));
        serverAddr.sin_family = AF_INET;
        //if (debug) fprintf(stderr, "Sending on UDP port %hu\n", lbPort);
        serverAddr.sin_port = htons(port);
        //if (debug) fprintf(stderr, "Connecting to host %s\n", lbHost);
        serverAddr.sin_addr.s_addr = inet_addr(host);
        memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);

        fprintf(stderr, "Connection socket to host %s, port %hu\n", host, port);
        int err = connect(clientSocket, (const sockaddr *) &serverAddr, sizeof(struct sockaddr_in));
        if (err < 0) {
            if (debug) perror("Error connecting UDP socket:");
            close(clientSocket);
            return err;
        }
    }

    // set the don't fragment bit
#ifdef __linux__
    {
            int val = IP_PMTUDISC_DO;
            setsockopt(clientSocket, IPPROTO_IP, IP_MTU_DISCOVER, &val, sizeof(val));
    }
#endif


    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Create socket to send tick/event# update to control plane. No need for big buffers.
    if (sendSync) {
        if (useIPv6) {
            struct sockaddr_in6 serverAddr6;

            /* create a DGRAM (UDP) socket in the INET/INET6 protocol */
            if ((cpSocket = socket(AF_INET6, SOCK_DGRAM, 0)) < 0) {
                perror("creating IPv6 cp socket");
                return -1;
            }

            socklen_t size = sizeof(int);

            // Configure settings in address struct
            // Clear it out
            memset(&serverAddr6, 0, sizeof(serverAddr6));
            // it is an INET address
            serverAddr6.sin6_family = AF_INET6;
            // the port we are going to send to, in network byte order
            serverAddr6.sin6_port = htons(cpport);
            // the server IP address, in network byte order
            inet_pton(AF_INET6, cphost, &serverAddr6.sin6_addr);

            int err = connect(cpSocket, (const sockaddr *) &serverAddr6, sizeof(struct sockaddr_in6));
            if (err < 0) {
                if (debug) perror("Error connecting UDP socket:");
                close(cpSocket);
                exit(1);
            }
        }
        else {
            struct sockaddr_in serverAddr;

            // Create UDP socket
            if ((cpSocket = socket(PF_INET, SOCK_DGRAM, 0)) < 0) {
                perror("creating IPv4 cp socket");
                return -1;
            }

            // Configure settings in address struct
            memset(&serverAddr, 0, sizeof(serverAddr));
            serverAddr.sin_family = AF_INET;
            //if (debug) fprintf(stderr, "Sending on UDP port %hu\n", lbPort);
            serverAddr.sin_port = htons(cpport);
            //if (debug) fprintf(stderr, "Connecting to host %s\n", lbHost);
            serverAddr.sin_addr.s_addr = inet_addr(cphost);
            memset(serverAddr.sin_zero, '\0', sizeof serverAddr.sin_zero);

            fprintf(stderr, "Connection socket to host %s, port %hu\n", cphost, cpport);
            int err = connect(cpSocket, (const sockaddr *) &serverAddr, sizeof(struct sockaddr_in));
            if (err < 0) {
                if (debug) perror("Error connecting UDP socket:");
                close(cpSocket);
                return err;
            }
        }
    }


    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Start thread to do rate printout
    pthread_t thd;
    int status = pthread_create(&thd, NULL, thread, (void *) nullptr);
    if (status != 0) {
        fprintf(stderr, "\n ******* error creating thread\n\n");
        return -1;
    }


    ///////////////////////////////////////////////////////////////////////////////////////////////
    int err;
    bool firstBuffer = true;
    bool lastBuffer  = true;
    delayCounter = delayPrescale;

    fprintf(stdout, "delay prescale = %u\n", delayPrescale);

    // Statistics & rate setting
    int64_t packetsSent=0;
    int64_t elapsed, microSecItShouldTake;
    uint64_t syncTime;
    struct timespec t1, t2, tStart, tEnd;
    int64_t excessTime, lastExcessTime = 0, buffersAtOnce, countDown;

    if (setByteRate || setBufRate) {
        // Don't send more than about 500k consecutive bytes with no delays to avoid overwhelming UDP bufs
        int64_t bytesToWriteAtOnce = 500000;

        if (setByteRate) {
            // Fixed the BYTE rate when making performance measurements.
            bufRate = byteRate / bufSize;
            fprintf(stderr, "packetBlaster: set byte rate = %" PRIu64 ", buf rate = %" PRId64 ", initial buf size = %" PRId64 "\n",
                    byteRate, bufRate, bufSize);
            // In this case we may need to adjust the buffer size to get the exact data rate.
            bufSize = byteRate / bufRate;
            fprintf(stderr, "packetBlaster: set byte rate = %" PRIu64 ", buf rate = %" PRId64 ", adjusted buf size = %" PRId64 "\n",
                    byteRate, bufRate, bufSize);

            fprintf(stderr, "packetBlaster: buf rate = %" PRIu64 ", buf size = %" PRIu64 ", data rate = %" PRId64 "\n",
                    bufRate, bufSize, byteRate);

            buffersAtOnce = 500000 / bufSize;
            if (buffersAtOnce  < 1) buffersAtOnce = 1;
            bytesToWriteAtOnce = buffersAtOnce * bufSize;
        }
        else {
            // Fixed the BUFFER rate since data rates may vary between data sources, but
            // the # of buffers sent need to be identical between those sources.
            byteRate = bufRate * bufSize;
            buffersAtOnce = bytesToWriteAtOnce / bufSize;

            fprintf(stderr, "packetBlaster: buf rate = %" PRIu64 ", buf size = %" PRIu64 ", data rate = %" PRId64 "\n",
                    bufRate, bufSize, byteRate);
        }

        countDown = buffersAtOnce;

        // musec to write data at desired rate
        microSecItShouldTake = 1000000L * bytesToWriteAtOnce / byteRate;
        fprintf(stderr,
                "packetBlaster: bytesToWriteAtOnce = %" PRId64 ", byteRate = %" PRId64 ", buffersAtOnce = %" PRId64 ", microSecItShouldTake = %" PRId64 "\n",
                bytesToWriteAtOnce, byteRate, buffersAtOnce, microSecItShouldTake);
    }


    if (setByteRate || setBufRate || sendSync) {
        // Start the clock
        clock_gettime(CLOCK_MONOTONIC, &t1);
        tStart = t1;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////

    uint64_t evtRate;
    uint64_t bufsSent = 0UL;

    while (true) {

        // If we're sending buffers at a constant rate AND we've sent the entire bunch
        if ((setByteRate || setBufRate) && countDown-- <= 0) {
            // Get the current time
            clock_gettime(CLOCK_MONOTONIC, &t2);
            // Time taken to send a bunch of buffers
            elapsed = 1000000L * (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec)/1000L;
            // Time yet needed in order for everything we've sent to be at the correct rate
            excessTime = microSecItShouldTake - elapsed + lastExcessTime;

//fprintf(stderr, "packetBlaster: elapsed = %lld, this excessT = %lld, last excessT = %lld, buffers/sec = %llu\n",
//        elapsed, (microSecItShouldTake - elapsed), lastExcessTime, buffersAtOnce*1000000/elapsed);

            // Do we need to wait before sending the next bunch of buffers?
            if (excessTime > 0) {
                // We need to wait, but it's possible that after the following delay,
                // we will have waited too long. We know this since any specified sleep
                // period is always a minimum.
                // If that's the case, in the next cycle, excessTime will be < 0.
                std::this_thread::sleep_for(std::chrono::microseconds(excessTime));
                // After this wait, we'll do another round of buffers to send,
                // but we need to start the clock again.
                clock_gettime(CLOCK_MONOTONIC, &t1);
                // Check to see if we overslept so correction can be done
                elapsed = 1000000L * (t1.tv_sec - t2.tv_sec) + (t1.tv_nsec - t2.tv_nsec)/1000L;
                lastExcessTime = excessTime - elapsed;
            }
            else {
                // If we're here, it took longer to send buffers than required in order to meet the
                // given buffer rate. So, it's likely that the specified rate is too high for this node.
                // Record any excess previous sleep time so it can be compensated for in next go round
                // if that is even possible.
                lastExcessTime = excessTime;
                t1 = t2;
            }
            countDown = buffersAtOnce - 1;
        }

        // Generate spread in backend processing time
        if (useTimeSpread) {
            backendTime = (uint32_t) timeDist(gen);
        }

        // Generate spread in buffer size
        if (useSizeSpread) {
            bufByteSize = (uint32_t) bufDist(gen);
        }

        err = sendPacketizedBuf(bufByteSize, maxUdpPayload, backendTime, clientSocket,
                                tick, protocol, entropy, version, dataId,
                                packetDelay, delayPrescale, &delayCounter,
                                debug, &packetsSent);
        if (err < 0) {
            // Should be more info in errno
            fprintf(stderr, "\nsendPacketizedBuffer: errno = %d, %s\n\n", errno, strerror(errno));
            exit(1);
        }

        bufsSent++;
        totalBytes += bufByteSize;
        totalPackets += packetsSent;
        totalEvents++;
        offset = 0;
        tick += tickPrescale;

        if (sendSync) {
            clock_gettime(CLOCK_MONOTONIC, &tEnd);
            syncTime = 1000000000UL * (tEnd.tv_sec - tStart.tv_sec) + (tEnd.tv_nsec - tStart.tv_nsec);

            // if >= 1 sec ...
            if (syncTime >= 1000000000UL) {
                // Calculate buf or event rate in Hz
                evtRate = bufsSent*1000000000/syncTime;

                // Send sync message to same destination
if (debug) fprintf(stderr, "send tick %" PRIu64 ", evtRate %" PRIu64 "\n\n", tick, evtRate);
                setSyncData(syncBuf, version, dataId, tick, evtRate, syncTime);
                err = send(cpSocket, syncBuf, 28, 0);
                if (err == -1) {
                    fprintf(stderr, "\npacketBlasterNew: error sending sync, errno = %d, %s\n\n", errno, strerror(errno));
                    return (-1);
                }

                tStart = tEnd;
                bufsSent = 0;
            }
        }


        // delay if any
        if (bufDelay) {
            if (--delayCounter < 1) {
                std::this_thread::sleep_for(std::chrono::microseconds(bufferDelay));
                delayCounter = delayPrescale;
            }
        }

    }

    return 0;
}
