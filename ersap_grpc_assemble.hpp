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
 * Contains routines to receive UDP packets that have been "packetized"
 * (broken up into smaller UDP packets by an EJFAT packetizer).
 * The receiving program handles sequentially numbered packets that may arrive out-of-order
 * coming from an FPGA-based between this and the sending program. Note that the routines
 * to reassemble buffers assume the new, version 2, RE headers. The code to reassemble the
 * older style RE header is still included but commented out.
 */
#ifndef EJFAT_ASSEMBLE_ERSAP_GRPC_H
#define EJFAT_ASSEMBLE_ERSAP_GRPC_H


#include <cstdio>
#include <string>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <ctime>
#include <cerrno>
#include <map>
#include <cmath>
#include <memory>
#include <getopt.h>
#include <climits>
#include <cinttypes>
#include <unordered_map>

#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>


#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>


#ifdef __APPLE__
#include <cctype>
#endif

// Reassembly (RE) header size in bytes
#define HEADER_BYTES 20
#define HEADER_BYTES_OLD 18

#define btoa(x) ((x)?"true":"false")


#ifdef __linux__
    // for recvmmsg
    #ifndef _GNU_SOURCE
        #define _GNU_SOURCE
    #endif

    #define htonll(x) ((1==htonl(1)) ? (x) : (((uint64_t)htonl((x) & 0xFFFFFFFFUL)) << 32) | htonl((uint32_t)((x) >> 32)))
    #define ntohll(x) ((1==ntohl(1)) ? (x) : (((uint64_t)ntohl((x) & 0xFFFFFFFFUL)) << 32) | ntohl((uint32_t)((x) >> 32)))
#endif


#ifdef __APPLE__

// Put this here so we can compile on MAC
struct mmsghdr {
    struct msghdr msg_hdr;  /* Message header */
    unsigned int  msg_len;  /* Number of received bytes for header */
};

extern int recvmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen,
                    int flags, struct timespec *timeout);

#endif


//#ifndef EJFAT_BYTESWAP_H
//#define EJFAT_BYTESWAP_H
//
//static inline uint16_t bswap_16(uint16_t x) {
//    return (x>>8) | (x<<8);
//}
//
//static inline uint32_t bswap_32(uint32_t x) {
//    return (bswap_16(x&0xffff)<<16) | (bswap_16(x>>16));
//}
//
//static inline uint64_t bswap_64(uint64_t x) {
//    return (((uint64_t)bswap_32(x&0xffffffffull))<<32) |
//           (bswap_32(x>>32));
//}
//#endif

    namespace ejfat {


        // Implementation of a fixed size, blocking queue found at:
        // https://morestina.net/blog/1400/minimalistic-blocking-bounded-queue-for-c
        // Note: Using this queue still requires each buffer to be allocated then freed.
        // To get better performance, it's best to use the ET system (shared memory) or
        // the Disruptor (preallocated array of objects).
        // This is small and convenient for now.

        template<typename T>
        class queue {
            std::deque<T> content;
            size_t capacity;

            std::mutex mutex;
            std::condition_variable not_empty;
            std::condition_variable not_full;

            queue(const queue &) = delete;
            queue(queue &&) = delete;
            queue &operator = (const queue &) = delete;
            queue &operator = (queue &&) = delete;

        public:
            queue(size_t capacity): capacity(capacity) {}

            void push(T &&item) {
                {
                    std::unique_lock<std::mutex> lk(mutex);
                    not_full.wait(lk, [this]() { return content.size() < capacity; });
                    content.push_back(std::move(item));
                }
                //not_empty.notify_one();
                not_empty.notify_all();
            }

            bool try_push(T &&item) {
                {
                    std::unique_lock<std::mutex> lk(mutex);
                    if (content.size() == capacity)
                        return false;
                    content.push_back(std::move(item));
                }
                //not_empty.notify_one();
                not_empty.notify_all();
                return true;
            }

            void pop(T &item) {
                {
                    std::unique_lock<std::mutex> lk(mutex);
                    not_empty.wait(lk, [this]() { return !content.empty(); });
                    item = std::move(content.front());
                    content.pop_front();
                }
                //not_full.notify_one();
                not_full.notify_all();
            }

            bool try_pop(T &item) {
                {
                    std::unique_lock<std::mutex> lk(mutex);
                    if (content.empty())
                        return false;
                    item = std::move(content.front());
                    content.pop_front();
                }
                //not_full.notify_one();
                not_full.notify_all();
                return true;
            }

            size_t size() {
                std::unique_lock<std::mutex> lk(mutex);
                return content.size();
            }

        };



        enum errorCodes {
            RECV_MSG = -1,
            TRUNCATED_MSG = -2,
            BUF_TOO_SMALL = -3,
            OUT_OF_ORDER = -4,
            BAD_FIRST_LAST_BIT = -5,
            OUT_OF_MEM = -6,
            BAD_ARG = -7,
            NO_REASSEMBLY = -8,
            NETWORK_ERROR = -9,
            INTERNAL_ERROR = -10
        };



        /**
         * Structure able to hold stats of packet-related quantities for receiving.
         * The contained info relates to the reading/reassembly of a complete buffer.
         */
        typedef struct packetRecvStats_t {
            volatile int64_t  endTime;          /**< Start time in microsec from clock_gettime. */
            volatile int64_t  startTime;        /**< End time in microsec from clock_gettime. */
            volatile int64_t  readTime;         /**< Microsec taken to read (all packets forming) one complete buffer. */

            volatile int64_t droppedPackets;   /**< Number of dropped packets. This cannot be known exactly, only estimate. */
            volatile int64_t acceptedPackets;  /**< Number of packets successfully read. */
            volatile int64_t discardedPackets; /**< Number of bytes discarded because reassembly was impossible. */

            volatile int64_t droppedBytes;     /**< Number of bytes dropped. */
            volatile int64_t acceptedBytes;    /**< Number of bytes successfully read, NOT including RE header. */
            volatile int64_t discardedBytes;   /**< Number of bytes dropped. */

            volatile int64_t droppedBuffers;    /**< Number of ticks/buffers for which no packets showed up.
                                                      Don't think it's possible to measure this in general. */
            volatile int64_t discardedBuffers;  /**< Number of ticks/buffers discarded. */
            volatile int64_t builtBuffers;      /**< Number of ticks/buffers fully reassembled. */

//            volatile int64_t discardedBuiltBufs;  /**< Number of fully reassembled buffers discarded due to full Q. */
//            volatile int64_t discardedBuiltPkts;  /**< Number of packets in fully reassembled buffers discarded due to full Q. */
//            volatile int64_t discardedBuiltBytes; /**< Number of bytes in fully reassembled buffers discarded due to full Q. */


            volatile int cpuPkt;               /**< CPU that thread to read pkts is running on. */
            volatile int cpuBuf;               /**< CPU that thread to read build buffers is running on. */
        } packetRecvStats;


        /**
         * Clear packetRecvStats structure.
         * @param stats pointer to structure to be cleared.
         */
        static void clearStats(packetRecvStats *stats) {
            stats->endTime = 0;
            stats->startTime = 0;
            stats->readTime = 0;

            stats->droppedPackets = 0;
            stats->acceptedPackets = 0;
            stats->discardedPackets = 0;

            stats->droppedBytes = 0;
            stats->acceptedBytes = 0;
            stats->discardedBytes = 0;

            stats->droppedBuffers = 0;
            stats->discardedBuffers = 0;
            stats->builtBuffers = 0;

//            stats->discardedBuiltBufs  = 0;
//            stats->discardedBuiltPkts  = 0;
//            stats->discardedBuiltBytes = 0;

            stats->cpuPkt = -1;
            stats->cpuBuf = -1;
        }

        /**
         * Clear packetRecvStats structure.
         * @param stats shared pointer to structure to be cleared.
         */
        static void clearStats(std::shared_ptr<packetRecvStats> stats) {
            clearStats(stats.get());
        }




        /**
         * Print some of the given packetRecvStats structure.
         * @param stats shared pointer to structure to be printed.
         */
        static void printStats(std::shared_ptr<packetRecvStats> const & stats, std::string const & prefix) {
            if (!prefix.empty()) {
                fprintf(stderr, "%s: ", prefix.c_str());
            }
            fprintf(stderr,  "bytes = %" PRId64 ", pkts = %" PRId64 ", dropped bytes = %" PRId64 ", dropped pkts = %" PRId64 ", dropped ticks = %" PRId64 "\n",
                    stats->acceptedBytes, stats->acceptedPackets, stats->droppedBytes,
                    stats->droppedPackets, stats->droppedBuffers);
        }


        /**
         * This routine takes a pointer and prints out (to stderr) the desired number of bytes
         * from the given position, in hex.
         *
         * @param data      data to print out
         * @param bytes     number of bytes to print in hex
         * @param label     a label to print as header
         */
        static void printBytes(const char *data, uint32_t bytes, const char *label) {

            if (label != nullptr) fprintf(stderr, "%s:\n", label);

            if (bytes < 1) {
                fprintf(stderr, "<no bytes to print ...>\n");
                return;
            }

            uint32_t i;
            for (i=0; i < bytes; i++) {
                if (i%8 == 0) {
                    fprintf(stderr, "\n  Buf(%3d - %3d) =  ", (i+1), (i + 8));
                }
                else if (i%4 == 0) {
                    fprintf(stderr, "  ");
                }

                // Accessing buf in this way does not change position or limit of buffer
                fprintf(stderr, "%02x ",( ((int)(*(data + i))) & 0xff)  );
            }

            fprintf(stderr, "\n\n");
        }


        /**
         * This routine takes a file pointer and prints out (to stderr) the desired number of bytes
         * from the given file, in hex.
         *
         * @param data      data to print out
         * @param bytes     number of bytes to print in hex
         * @param label     a label to print as header
         */
        static void printFileBytes(FILE *fp, uint32_t bytes, const char *label) {

            long currentPos = ftell(fp);
            rewind(fp);
            uint8_t byte;


            if (label != nullptr) fprintf(stderr, "%s:\n", label);

            if (bytes < 1) {
                fprintf(stderr, "<no bytes to print ...>\n");
                return;
            }

            uint32_t i;
            for (i=0; i < bytes; i++) {
                if (i%10 == 0) {
                    fprintf(stderr, "\n  Buf(%3d - %3d) =  ", (i+1), (i + 10));
                }
                else if (i%5 == 0) {
                    fprintf(stderr, "  ");
                }

                // Accessing buf in this way does not change position or limit of buffer
                fread(&byte, 1, 1, fp);
                fprintf(stderr, "  0x%02x ", byte);
            }

            fprintf(stderr, "\n\n");
            fseek(fp, currentPos, SEEK_SET);
        }


        /**
         * Parse the load balance header at the start of the given buffer.
         * This routine will, most likely, never be used as this header is
         * stripped off and parsed in the load balancer and the user never
         * sees it.
         *
         * <pre>
         *  protocol 'L:8,B:8,Version:8,Protocol:8,Reserved:16,Entropy:16,Tick:64'
         *
         *  0                   1                   2                   3
         *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  |       L       |       B       |    Version    |    Protocol   |
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  3               4                   5                   6
         *  2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  |              Rsvd             |            Entropy            | channel id
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  6                                               12
         *  4 5       ...           ...         ...         0 1 2 3 4 5 6 7
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  |                                                               |
         *  +                              Tick                             +
         *  |                                                               |
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * </pre>
         *
         * @param buffer   buffer to parse.
         * @param ll       return 1st byte as char.
         * @param bb       return 2nd byte as char.
         * @param version  return 3rd byte as integer version.
         * @param protocol return 4th byte as integer protocol.
         * @param entropy  return 2 bytes as 16 bit integer entropy.
         * @param tick     return last 8 bytes as 64 bit integer tick.
         */
        static void parseLbHeader(const char* buffer, char* ll, char* bb,
                                  uint32_t* version, uint32_t* protocol,
                                  uint32_t* entropy, uint64_t* tick)
        {
            *ll = buffer[0];
            *bb = buffer[1];
            if ((*ll != 'L') || (*bb != 'B')) {
                throw std::runtime_error("ersap pkt does not start with 'LB'");
            }

            *version  = ((uint32_t)buffer[2] & 0xff);
            *protocol = ((uint32_t)buffer[3] & 0xff);
            *entropy  = ntohs(*((uint16_t *)(&buffer[6]))) & 0xffff;
            *tick     = ntohll(*((uint64_t *)(&buffer[8])));
        }



        /**
         * Parse the reassembly header at the start of the given buffer.
         * Return parsed values in pointer args.
         * This is the new, version 2, RE header.
         *
         * <pre>
         *  protocol 'Version:4, Rsvd:12, Data-ID:16, Offset:32, Length:32, Tick:64'
         *
         *  0                   1                   2                   3
         *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  |Version|        Rsvd           |            Data-ID            |
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  |                         Buffer Offset                         |
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  |                         Buffer Length                         |
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         *  |                                                               |
         *  +                             Tick                              +
         *  |                                                               |
         *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
         * </pre>
         *
         * @param buffer   buffer to parse.
         * @param version  returned version.
         * @param dataId   returned data source id.
         * @param offset   returned byte offset into buffer of this data payload.
         * @param length   returned total buffer length in bytes of which this packet is a port.
         * @param tick     returned tick value, also in LB meta data.
         */
        static void parseReHeader(const char* buffer, int* version, uint16_t* dataId,
                                  uint32_t* offset, uint32_t* length, uint64_t *tick)
        {
            // Now pull out the component values
            *version = (buffer[0] >> 4) & 0xf;
            *dataId  = ntohs(*((uint16_t *)  (buffer + 2)));
            *offset  = ntohl(*((uint32_t *)  (buffer + 4)));
            *length  = ntohl(*((uint32_t *)  (buffer + 8)));
            *tick    = ntohll(*((uint64_t *) (buffer + 12)));
        }



       /**
        * <p>
        * Parse the data of a synchronization message sent directly to the load balancer's CP.
        * The first 3 fields are as ordered. The srcId, evtNum, evtRate and time are all
        * in network byte order.</p>
        * Implemented <b>without</b> using C++ bit fields.
        *
        * <pre>
        *  protocol 'Version:4, Rsvd:12, Data-ID:16, Offset:32, Length:32, Tick:64'
        *
        *    0                   1                   2                   3
        *    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
        *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        *    |       L       |       C       |    Version    |      Rsvd     |
        *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        *    |                           EventSrcId                          |
        *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        *    |                                                               |
        *    +                          EventNumber                          +
        *    |                                                               |
        *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        *    |                         AvgEventRateHz                        |
        *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        *    |                                                               |
        *    +                          UnixTimeNano                         +
        *    |                                                               |
        *    +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        * </pre>
        *
        * @param buffer   data buffer.
        * @param version  filled with version of the software used to send this msg.
        * @param srcId    filled iwth id number of data source.
        * @param evtNum   filled with unsigned 64 bit event number used to tell the load balancer
        *                 that the sending data source has already sent this, latest, event.
        * @param evtRate  filled with the rate, in Hz, that the sending data source is sending events
        *                 to the load balancer (0 if unknown).
        * @param nanos    filled with the unix time in nanoseconds that this message sent (0 if unknown).
        */
       static void parseSyncData(const char *buffer, uint32_t *version, uint32_t *srcId,
                                 uint64_t *evtNum, uint32_t *evtRate, uint64_t *nanos) {

           *version  = buffer[2] & 0xff;
           *srcId    = ntohl (*((uint32_t *)  (buffer + 4)));
           *evtNum   = ntohll(*((uint64_t *)  (buffer + 8)));
           *evtRate  = ntohl (*((uint32_t *)  (buffer + 16)));
           *nanos    = ntohll(*((uint64_t *)  (buffer + 20)));
       }



       /**
        * Parse the data in each packet from simSender.
        * Return parsed values in pointer args.
        * All data is in network byte order.
        *
        * <pre>
        *
        *  0                   1                   2                   3
        *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
        *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        *  |                     Processing Delay (microsec)               |
        *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        *  |              Total # of packets for this event                |
        *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        *  |              Packet sequence # for this event                 |
        *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        * </pre>
        *
        * @param buffer      buffer to parse.
        * @param delay       returned microsec delay to simulate backend processing.
        * @param totatPkts   returned total number of packets making up this event.
        * @param pktSequence returned packet sequence number for this event.
        */
        static void parsePacketData(const char* buffer, uint32_t* delay,
                                    uint32_t* totalPkts, uint32_t* pktSequence)
        {
            // Now pull out the component values
            *delay       = ntohl(*((uint32_t *)  (buffer)));
            *totalPkts   = ntohl(*((uint32_t *)  (buffer + 4)));
            *pktSequence = ntohl(*((uint32_t *)  (buffer + 8)));
        }





        /**
        * <p>
        * Assemble incoming packets into the array backing the given buffer.
        * It will read return on reading the next entire buffer or on error.
        * Will work best on small / reasonably sized buffers.
        * This routine allows for out-of-order packets if they don't cross tick boundaries.
        * This assumes the new, version 2, RE header.
        * Data can only come from 1 source, which is returned in the dataId value-result arg.
        * Data from a source other than that of the first packet will be ignored.
        * </p>
        *
        * <p>
        * If the given tick value is <b>NOT</b> 0xffffffffffffffff, then it is the next expected tick.
        * And in this case, this method makes an attempt at figuring out how many buffers and packets
        * were dropped using tickPrescale.
        * </p>
        *
        * <p>
        * A note on statistics. The raw counts are <b>ADDED</b> to what's already
        * in the stats structure. It's up to the user to clear stats before calling
        * this method if desired.
        * </p>
        *
        * @param dataBuf           place to store assembled packets.
        * @param bufLen            byte length of dataBuf.
        * @param udpSocket         UDP socket to read.
        * @param debug             turn debug printout on & off.
        * @param tick              value-result parameter which gives the next expected tick
        *                          and returns the tick that was built. If it's passed in as
        *                          0xffff ffff ffff ffff, then ticks are coming in no particular order.
        * @param dataId            to be filled with data ID from RE header (can be nullptr).
        * @param stats             to be filled packet statistics.
        * @param tickPrescale      add to current tick to get next expected tick.
        *
        * @return total data bytes read (does not include RE header).
        *         If there error in recvfrom, return RECV_MSG.
        *         If buffer is too small to contain reassembled data, return BUF_TOO_SMALL.
        *         If a pkt contains too little data, return INTERNAL_ERROR.
        */
        static ssize_t getReassembledBuffer(std::vector<char> &vec, int udpSocket,
                                            bool debug, uint64_t *tick, uint16_t *dataId,
                                            std::shared_ptr<packetRecvStats> stats,
                                            uint32_t tickPrescale) {

            uint64_t prevTick = UINT_MAX;
            uint64_t expectedTick = *tick;
            uint64_t packetTick;

            size_t bufLen = vec.capacity();
            char* dataBuf = vec.data();

            uint32_t offset, length = 0, prevLength, pktCount, pktSequence;
            uint32_t delay, totalPkts = 0, prevTotalPkts;

            bool dumpTick = false;
            bool veryFirstRead = true;

            int  version;
            uint16_t packetDataId, srcId;
            ssize_t dataBytes, bytesRead, totalBytesRead = 0;

            // stats
            bool knowExpectedTick = expectedTick != 0xffffffffffffffffL;
            bool takeStats = stats != nullptr;
            int64_t discardedPackets = 0, discardedBytes = 0, discardedBufs = 0;

            // Storage for packet
            char pkt[9100];


            if (debug && takeStats) fprintf(stderr, "getReassembledBuffer: buf size = %lu, take stats = %d\n",
                                            bufLen, takeStats);

            while (true) {

                if (veryFirstRead) {
                    totalBytesRead = 0;
                    pktCount = 0;
                }

                // Read UDP packet
                bytesRead = recvfrom(udpSocket, pkt, 9100, 0, nullptr, nullptr);
                if (bytesRead < 0) {
                    if (debug) fprintf(stderr, "getReassembledBuffer: recvmsg failed: %s\n", strerror(errno));
                    return (RECV_MSG);
                }
                else if (bytesRead < HEADER_BYTES) {
                    if (debug) fprintf(stderr, "getReassembledBuffer: packet does not contain not enough data\n");
                    return (INTERNAL_ERROR);
                }
                dataBytes = bytesRead - HEADER_BYTES;


                // Parse RE header
                prevLength = length;
                parseReHeader(pkt, &version, &packetDataId, &offset, &length, &packetTick);
                if (veryFirstRead) {
                    // record data id of first packet of buffer
                    srcId = packetDataId;
                }
                else if (packetDataId != srcId) {
                    // different data source, reject this packet
                    if (debug) fprintf(stderr, "getReassembledBuffer: reject packet from source id %hu\n", packetDataId);
                    continue;
                }


                // Parse data
                prevTotalPkts = totalPkts;
                parsePacketData(pkt + HEADER_BYTES, &delay, &totalPkts, &pktSequence);
                if (debug && takeStats) {
                    fprintf(stderr, "getReassembledBuffer: delay = %u, pkts = %u, seq = %u, tick = %" PRIu64 ", srcid = %hu\n",
                                               delay, totalPkts, pktSequence, packetTick, packetDataId);
                }


                // The following if-else is built on the idea that we start with a packet that has offset = 0.
                // While it's true that, if missing, it may be out-of-order and will show up eventually,
                // experience has shown that this almost never happens. Thus, for efficiency's sake,
                // we automatically dump any tick whose first packet does not show up FIRST.

                // Probably, where this most often gets us into trouble is if the first packet of the next
                // tick/event shows up just before the last pkt of the previous tick. In that case, this logic
                // just dumps all the previous info even if last pkt comes a little late.

                // Worst case scenario is if the pkts of 2 events are interleaved.
                // Then the number of dumped packets, bytes, and events will be grossly over-counted.

                // To do a complete job of trying to track out-of-order packets, we would need to
                // simultaneously keep track of packets from multiple ticks. This small routine
                // would need to keep state - greatly complicating things. So skip that here.
                // Such work is done in the packetBlasteeFull.cc program.

                // In general, tracking dropped pkts/events/data will always be guess work unless
                // we know exactly what we're supposed to be receiving.
                // Thus, normally we cannot know how many complete events were dropped.
                // When deciding to drop an event due to incomplete packets, we attempt to
                // get a guess on the # of packets.
                // In this simulation, however, the # of packets are sent as part of the data!


                // If we get packet from new tick ...
                if (packetTick != prevTick) {
                    // If we're here, either we've just read the very first legitimate packet,
                    // or we've dropped some packets and advanced to another tick.

                    if (offset != 0) {
                        // Already have trouble, looks like we dropped the first packet of this new tick,
                        // and possibly others after it.
                        // So go ahead and dump the rest of the tick in an effort to keep any high data rate.
                        if (debug)
                            fprintf(stderr, "Skip pkt from id %hu, %" PRIu64 " - %u, expected seq 0\n",
                                packetDataId, packetTick, offset);

                        // Go back to read beginning of buffer
                        veryFirstRead = true;
                        dumpTick = true;
                        prevTick = packetTick;

                        // Stats. Guess at # of packets, rounding up
                        discardedPackets += totalPkts;
                        discardedBytes += length;
                        discardedBufs++;

                        continue;
                    }

                    if (!veryFirstRead) {
                        // The last tick's buffer was not fully contructed
                        // before this new tick showed up!
                        if (debug) fprintf(stderr, "Discard tick %" PRIu64 "\n", prevTick);

                        pktCount = 0;
                        totalBytesRead = 0;
                        srcId = packetDataId;

                        // We discard previous tick/event
                        discardedPackets += prevTotalPkts;
                        discardedBytes += prevLength;
                        discardedBufs++;
                    }

                    // If here, new tick/event/buffer, offset = 0.
                    // There's a chance we can construct a full buffer.
                    // Overwrite everything we saved from previous tick.
                    dumpTick = false;
                }
                else if (dumpTick) {
                    // Same as last tick.
                    // If here, we missed beginning pkt(s) for this buf so we're dumping whole tick
                    veryFirstRead = true;

                    if (debug) fprintf(stderr, "Dump pkt from id %hu, tick %" PRIu64 "\n",
                                       packetDataId, packetTick);
                    continue;
                }


                // After reading very first pkt, check to see if we have enough memory to read in the whole event.
                // If not, expand it.
                if (veryFirstRead && length > bufLen) {
                    if (debug) fprintf(stderr, "getReassembledBuffer: expand vector to hold %u bytes\n", length);
                    vec.reserve(length);
                    bufLen = length;
                    dataBuf = vec.data();
                }


                // Copy data into buf at correct location (provided by RE header)
                memcpy(dataBuf + offset, pkt + HEADER_BYTES, dataBytes);


                // At this point we do something clever. We record the packet number
                // and write it into the data buffer - just after the first pkt's 3 data ints.
                // This way we preserve exactly what came in and in what order.
                // Just use local byte order since it's only going to be read by another thd in this process.
                memcpy(dataBuf + 12 + 4*pktCount, &pktSequence, 4);


                totalBytesRead += dataBytes;
                veryFirstRead = false;
                prevTick = packetTick;
                pktCount++;

//fprintf(stderr, "          %u\n", pktCount);

                // If we've reassembled all packets ...
                if (pktCount >= totalPkts) {
                    // Done
                    *tick = packetTick;
                    if (dataId != nullptr) *dataId = packetDataId;

                    // Keep some stats
                    if (takeStats) {
                        if (knowExpectedTick) {
                            int64_t diff = packetTick - expectedTick;
                            diff = (diff < 0) ? -diff : diff;
                            int64_t droppedTicks = diff / tickPrescale;

                            // In this case, it includes the discarded bufs (which it should not)
                            stats->droppedBuffers   += droppedTicks; // estimate

                            // This works if all the buffers coming in are exactly the same size.
                            // If they're not, then the # of packets of this buffer
                            // is used to guess at how many packets were dropped for the dropped tick(s).
                            // Again, this includes discarded packets which it should not.
                            stats->droppedPackets += droppedTicks * pktCount;
                        }

                        stats->acceptedBytes    += totalBytesRead;
                        stats->acceptedPackets  += pktCount;
//fprintf(stderr, "             %" PRId64 "\n", stats->acceptedPackets);

                        stats->discardedBytes   += discardedBytes;
                        stats->discardedPackets += discardedPackets;
                        stats->discardedBuffers += discardedBufs;
                    }

                    break;
                }
            }

            return totalBytesRead;
        }








    }


#endif // EJFAT_ASSEMBLE_ERSAP_GRPC_H
