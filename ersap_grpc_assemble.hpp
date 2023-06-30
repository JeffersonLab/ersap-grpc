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
                not_empty.notify_one();
            }

            bool try_push(T &&item) {
                {
                    std::unique_lock<std::mutex> lk(mutex);
                    if (content.size() == capacity)
                        return false;
                    content.push_back(std::move(item));
                }
                not_empty.notify_one();
                return true;
            }

            void pop(T &item) {
                {
                    std::unique_lock<std::mutex> lk(mutex);
                    not_empty.wait(lk, [this]() { return !content.empty(); });
                    item = std::move(content.front());
                    content.pop_front();
                }
                not_full.notify_one();
            }

            bool try_pop(T &item) {
                {
                    std::unique_lock<std::mutex> lk(mutex);
                    if (content.empty())
                        return false;
                    item = std::move(content.front());
                    content.pop_front();
                }
                not_full.notify_one();
                return true;
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

            volatile int32_t droppedBuffers;    /**< Number of ticks/buffers for which no packets showed up.
                                                      Don't think it's possible to measure this in general. */
            volatile int32_t discardedBuffers;  /**< Number of ticks/buffers discarded. */
            volatile int32_t builtBuffers;      /**< Number of ticks/buffers fully reassembled. */

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

            stats->cpuPkt = -1;
            stats->cpuBuf = -1;
        }

        /**
         * Clear packetRecvStats structure.
         * @param stats shared pointer to structure to be cleared.
         */
        static void clearStats(std::shared_ptr<packetRecvStats> stats) {
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

            stats->cpuPkt = -1;
            stats->cpuBuf = -1;
            stats->cpuBuf = -1;
        }




        /**
         * Print some of the given packetRecvStats structure.
         * @param stats shared pointer to structure to be printed.
         */
        static void printStats(std::shared_ptr<packetRecvStats> const & stats, std::string const & prefix) {
            if (!prefix.empty()) {
                fprintf(stderr, "%s: ", prefix.c_str());
            }
            fprintf(stderr,  "bytes = %" PRIu64 ", pkts = %" PRIu64 ", dropped bytes = %" PRIu64 ", dropped pkts = %" PRIu64 ", dropped ticks = %u\n",
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
         * Routine to read a single UDP packet into a single buffer.
         * The reassembly header will be parsed and its data retrieved.
         * This uses the new, version 2, RE header.
         * </p>
         *
         * It's the responsibility of the caller to have at least enough space in the
         * buffer for 1 MTU of data. Otherwise, the caller risks truncating the data
         * of a packet and having error code of TRUNCATED_MSG returned.
         *
         *
         * @param dataBuf   buffer in which to store actual data read (not any headers).
         * @param bufLen    available bytes in dataBuf in which to safely write.
         * @param udpSocket UDP socket to read.
         * @param tick      to be filled with tick from RE header.
         * @param length    to be filled with buffer length from RE header.
         * @param offset    to be filled with buffer offset from RE header.
         * @param dataId    to be filled with data id read RE header.
         * @param version   to be filled with version read RE header.
         * @param last      to be filled with "last" bit id from RE header,
         *                  indicating the last packet in a series used to send data.
         * @param debug     turn debug printout on & off.
         *
         * @return number of data (not headers!) bytes read from packet.
         *         If there's an error in recvfrom, it will return RECV_MSG.
         *         If there is not enough data to contain a header, it will return INTERNAL_ERROR.
         *         If there is not enough room in dataBuf to hold incoming data, it will return BUF_TOO_SMALL.
         */
        static ssize_t readPacketRecvFrom(char *dataBuf, size_t bufLen, int udpSocket,
                                      uint64_t *tick, uint32_t *length, uint32_t* offset,
                                      uint16_t* dataId, int* version, bool debug) {

            // Storage for packet
            char pkt[9100];

            ssize_t bytesRead = recvfrom(udpSocket, pkt, 9100, 0, nullptr, nullptr);
            if (bytesRead < 0) {
                if (debug) fprintf(stderr, "recvmsg() failed: %s\n", strerror(errno));
                return(RECV_MSG);
            }
            else if (bytesRead < HEADER_BYTES) {
                fprintf(stderr, "recvfrom(): not enough data to contain a header on read\n");
                return(INTERNAL_ERROR);
            }

            if (bufLen < bytesRead) {
                return(BUF_TOO_SMALL);
            }

            // Parse header
            parseReHeader(pkt, version, dataId, offset, length, tick);

            // Copy datq
            ssize_t dataBytes = bytesRead - HEADER_BYTES;
            memcpy(dataBuf, pkt + HEADER_BYTES, dataBytes);

            return dataBytes;
        }




        /**
         * <p>
         * Assemble incoming packets into the given buffer.
         * It will read entire buffer or return an error.
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
        static ssize_t getCompletePacketizedBuffer(char* dataBuf, size_t bufLen, int udpSocket,
                                                   bool debug, uint64_t *tick, uint16_t *dataId,
                                                   std::shared_ptr<packetRecvStats> stats,
                                                   uint32_t tickPrescale) {

            uint64_t prevTick = UINT_MAX;
            uint64_t expectedTick = *tick;
            uint64_t packetTick;

            uint32_t offset, length, pktCount;

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


            if (debug && takeStats) fprintf(stderr, "getCompletePacketizedBuffer: buf size = %lu, take stats = %d, %p\n",
                                            bufLen, takeStats, stats.get());

            while (true) {

                if (veryFirstRead) {
                    totalBytesRead = 0;
                    pktCount = 0;
                }

                // Read UDP packet
                bytesRead = recvfrom(udpSocket, pkt, 9100, 0, nullptr, nullptr);
                if (bytesRead < 0) {
                    if (debug) fprintf(stderr, "getCompletePacketizedBuffer: recvmsg failed: %s\n", strerror(errno));
                    return (RECV_MSG);
                }
                else if (bytesRead < HEADER_BYTES) {
                    if (debug) fprintf(stderr, "getCompletePacketizedBuffer: packet does not contain not enough data\n");
                    return (INTERNAL_ERROR);
                }
                dataBytes = bytesRead - HEADER_BYTES;

                // Parse header
                parseReHeader(pkt, &version, &packetDataId, &offset, &length, &packetTick);
                if (veryFirstRead) {
                    // record data id of first packet of buffer
                    srcId = packetDataId;
                }
                else if (packetDataId != srcId) {
                    // different data source, reject this packet
                    continue;
                }

                // The following if-else is built on the idea that we start with a packet that has offset = 0.
                // While it's true that, if missing, it may be out-of-order and will show up eventually,
                // experience has shown that this almost never happens. Thus, for efficiency's sake,
                // we automatically dump any tick whose first packet does not show up FIRST.

                // To do a complete job of trying to track out-of-order packets, we would need to
                // simultaneously keep track of packets from multiple ticks. This small routine
                // would need to keep state - greatly complicating things. So skip that here.
                // Such work is done in the packetBlasteeNew2.cc program.

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

                        // stats
                        discardedPackets++;
                        discardedBytes += dataBytes;
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
                    }

                    // If here, new tick/buffer, offset = 0.
                    // There's a chance we can construct a full buffer.
                    // Overwrite everything we saved from previous tick.
                    dumpTick = false;
                }
                else if (dumpTick) {
                    // Same as last tick.
                    // If here, we missed beginning pkt(s) for this buf so we're dumping whole tick
                    veryFirstRead = true;

                    // stats
                    discardedPackets++;
                    discardedBytes += dataBytes;

                    if (debug) fprintf(stderr, "Dump pkt from id %hu, %" PRIu64 " - %u, expected seq 0\n",
                                       packetDataId, packetTick, offset);
                    continue;
                }

                // Check to see if there's room to write data into provided buffer
                if (offset + dataBytes > bufLen) {
                    if (debug) fprintf(stderr, "getCompletePacketizedBuffer: buffer too small to hold data\n");
                    return (BUF_TOO_SMALL);
                }

                // Copy data into buf at correct location (provided by RE header)
                memcpy(dataBuf + offset, pkt + HEADER_BYTES, dataBytes);

                totalBytesRead += dataBytes;
                veryFirstRead = false;
                prevTick = packetTick;
                pktCount++;

                // If we've written all data to this buf ...
                if (totalBytesRead >= length) {
                    // Done
                    *tick = packetTick;
                    if (dataId != nullptr) *dataId = packetDataId;

                    // Keep some stats
                    if (takeStats) {
                        int64_t diff = 0;
                        int64_t droppedTicks = 0UL;
                        if (knowExpectedTick) {
                            diff = packetTick - expectedTick;
                            diff = (diff < 0) ? -diff : diff;
                            droppedTicks = diff / tickPrescale;

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

                        stats->discardedBytes   += discardedBytes;
                        stats->discardedPackets += discardedPackets;
                        stats->discardedBuffers += discardedBufs;
                    }

                    break;
                }
            }

            return totalBytesRead;
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

            uint32_t offset, length, pktCount;
            uint32_t delay, totalPkts, pktSequence;

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


            if (debug && takeStats) fprintf(stderr, "getReassembledBuffer: buf size = %lu, take stats = %d, %p\n",
                                            bufLen, takeStats, stats.get());

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
                parseReHeader(pkt, &version, &packetDataId, &offset, &length, &packetTick);
                if (veryFirstRead) {
                    // record data id of first packet of buffer
                    srcId = packetDataId;
                }
                else if (packetDataId != srcId) {
                    // different data source, reject this packet
                    continue;
                }


                // Parse data
                parsePacketData(pkt + HEADER_BYTES, &delay, &totalPkts, &pktSequence);


                // The following if-else is built on the idea that we start with a packet that has offset = 0.
                // While it's true that, if missing, it may be out-of-order and will show up eventually,
                // experience has shown that this almost never happens. Thus, for efficiency's sake,
                // we automatically dump any tick whose first packet does not show up FIRST.

                // To do a complete job of trying to track out-of-order packets, we would need to
                // simultaneously keep track of packets from multiple ticks. This small routine
                // would need to keep state - greatly complicating things. So skip that here.
                // Such work is done in the packetBlasteeFull.cc program.

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

                        // stats
                        discardedPackets++;
                        discardedBytes += dataBytes;
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

                    // stats
                    discardedPackets++;
                    discardedBytes += dataBytes;

                    if (debug) fprintf(stderr, "Dump pkt from id %hu, %" PRIu64 " - %u, expected seq 0\n",
                            packetDataId, packetTick, offset);
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


                // If we've reassembled all packets ...
                if (pktCount >= totalPkts) {
                    // Done
                    *tick = packetTick;
                    if (dataId != nullptr) *dataId = packetDataId;

                    // Keep some stats
                    if (takeStats) {
                        int64_t diff = 0;
                        int64_t droppedTicks = 0UL;
                        if (knowExpectedTick) {
                            diff = packetTick - expectedTick;
                            diff = (diff < 0) ? -diff : diff;
                            droppedTicks = diff / tickPrescale;

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

                        stats->discardedBytes   += discardedBytes;
                        stats->discardedPackets += discardedPackets;
                        stats->discardedBuffers += discardedBufs;
                    }

                    break;
                }
            }

            return totalBytesRead;
        }





////////////////////////


        /**
         * <p>
         * Assemble incoming packets into a buffer that may be provided by the caller.
         * If it's null or if it ends up being too small,
         * the buffer will be created / reallocated and returned by this routine.
         * Will work best on small / reasonable sized buffers.
         * A internally allocated buffer is guaranteed to fit all reassembled data.
         * It's the responsibility of the user to free any buffer that is internally allocated.
         * If user gives nullptr for buffer and 0 for buffer length, buf defaults to internally
         * allocated 100kB. If the user provides a buffer < 9000 bytes, a larger one will be allocated.
         * This routine will read entire buffer or return an error.
         * </p>
         *
         * <p>
         * How does the caller determine if a buffer was (re)allocated in this routine?
         * If the returned buffer pointer is different than that supplied or if the supplied
         * buffer length is smaller than that returned, then the buffer was allocated
         * internally and must be freed by the caller.
         * </p>
         *
         * <p>
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
         * @param dataBufAlloc      value-result pointer to data buffer.
         *                          User-given buffer to store assembled packets or buffer
         *                          (re)allocated by this routine. If (re)allocated internally,
         *                          CALLER MUST FREE THIS BUFFER!
         * @param pBufLen           value-result pointer to byte length of dataBuf.
         *                          Buffer length of supplied buffer. If no buffer supplied, or if buffer
         *                          is (re)allocated, the length of the new buffer is passed back to caller.
         *                          In all cases, the buffer length is returned.
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
         *         If dataBufAlloc or bufLenPtr are null, return BAD_ARG.
         *         If receiving &gt; 99 pkts from wrong data id, return NO_REASSEMBLY.
         *         If error in recvfrom, return RECV_MSG.
         *         If cannot allocate memory, return OUT_OF_MEM.
         *         If on a read &lt; HEADER_BYTES data returned, return INTERNAL_ERROR.
         */
        static ssize_t getCompleteAllocatedBuffer(char** dataBufAlloc, size_t *pBufLen, int udpSocket,
                                                  bool debug, uint64_t *tick, uint16_t *dataId,
                                                  std::shared_ptr<packetRecvStats> stats,
                                                  uint32_t tickPrescale) {

            if (pBufLen == nullptr || dataBufAlloc == nullptr) {
                fprintf(stderr, "getCompletePacketizedBufferNew: null arg(s)\n");
                return BAD_ARG;
            }

            // Length of buf passed in, or suggested length for this routine to allocate
            size_t bufLen = *pBufLen;
            bool allocateBuf = false;

            // If we need to allocate buffer
            if (*dataBufAlloc == nullptr) {
                if (bufLen == 0) {
                    // Use default len of 100kB
                    bufLen = 100000;
                }
                else if (bufLen < 9000) {
                    // Make sure we can at least read one JUMBO packet
                    bufLen = 9000;
                }
                allocateBuf = true;
            }
            else {
                if (bufLen < 9000) {
                    bufLen = 9000;
                    allocateBuf = true;
                }
            }

            char *dataBuf = *dataBufAlloc;
            if (allocateBuf) {
                dataBuf = (char *) malloc(bufLen);
                if (dataBuf == nullptr) {
                    return OUT_OF_MEM;
                }
            }


            uint64_t prevTick = UINT_MAX;
            uint64_t expectedTick = *tick;
            uint64_t packetTick;

            uint32_t offset, length, pktCount, rejectedPkt = 0;

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

            if (debug && takeStats) fprintf(stderr, "getCompletePacketizedBufferNew: buf size = %lu, take stats = %d, %p\n",
                                            bufLen, takeStats, stats.get());

            while (true) {

                if (veryFirstRead) {
                    totalBytesRead = 0;
                    pktCount = 0;
                }

                // Read UDP packet
                bytesRead = recvfrom(udpSocket, pkt, 9100, 0, nullptr, nullptr);
                if (bytesRead < 0) {
                    if (debug) fprintf(stderr, "getCompletePacketizedBufferNew: recvmsg failed: %s\n", strerror(errno));
                    if (allocateBuf) {
                        free(dataBuf);
                    }
                    return (RECV_MSG);
                }
                else if (bytesRead < HEADER_BYTES) {
                    if (debug) fprintf(stderr, "getCompletePacketizedBufferNew: packet does not contain not enough data\n");
                    if (allocateBuf) {
                        free(dataBuf);
                    }
                    return (INTERNAL_ERROR);
                }
                dataBytes = bytesRead - HEADER_BYTES;

                // Parse header
                parseReHeader(pkt, &version, &packetDataId, &offset, &length, &packetTick);
                if (veryFirstRead) {
                    // record data id of first packet of buffer
                    srcId = packetDataId;
                }
                else if (packetDataId != srcId) {
                    // different data source, reject this packet
                    if (++rejectedPkt >= 100) {
                        // Return error if we've received at least 100 irrelevant packets
                        if (allocateBuf) {
                            free(dataBuf);
                        }
                        return (NO_REASSEMBLY);
                    }
                    continue;
                }

                // The following if-else is built on the idea that we start with a packet that has offset = 0.
                // While it's true that, if missing, it may be out-of-order and will show up eventually,
                // experience has shown that this almost never happens. Thus, for efficiency's sake,
                // we automatically dump any tick whose first packet does not show up FIRST.

                // To do a complete job of trying to track out-of-order packets, we would need to
                // simultaneously keep track of packets from multiple ticks. This small routine
                // would need to keep state - greatly complicating things. So skip that here.
                // Such work is done in the packetBlasteeNew2.cc program.

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

                        // stats
                        discardedPackets++;
                        discardedBytes += dataBytes;
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
                    }

                    // If here, new tick/buffer, offset = 0.
                    // There's a chance we can construct a full buffer.
                    // Overwrite everything we saved from previous tick.
                    dumpTick = false;
                }
                else if (dumpTick) {
                    // Same as last tick.
                    // If here, we missed beginning pkt(s) for this buf so we're dumping whole tick
                    veryFirstRead = true;

                    // stats
                    discardedPackets++;
                    discardedBytes += dataBytes;

                    if (debug) fprintf(stderr, "Dump pkt from id %hu, %" PRIu64 " - %u, expected seq 0\n",
                                       packetDataId, packetTick, offset);
                    continue;
                }

                // Check to see if there's room to write data into provided buffer
                if (offset + dataBytes > bufLen) {
                    // Not enough room! Double buffer size here
                    bufLen *= 2;
                    // realloc copies data over if necessary
                    dataBuf = (char *)realloc(dataBuf, bufLen);
                    if (dataBuf == nullptr) {
                        if (allocateBuf) {
                            free(dataBuf);
                        }
                        return OUT_OF_MEM;
                    }
                    allocateBuf = true;
                    if (debug) fprintf(stderr, "getCompletePacketizedBufferNew: reallocated buffer to %zu bytes\n", bufLen);
                }

                // Copy data into buf at correct location (provided by RE header)
                memcpy(dataBuf + offset, pkt + HEADER_BYTES, dataBytes);

                totalBytesRead += dataBytes;
                veryFirstRead = false;
                prevTick = packetTick;
                pktCount++;

                // If we've written all data to this buf ...
                if (totalBytesRead >= length) {
                    // Done
                    *tick = packetTick;
                    if (dataId != nullptr) *dataId = packetDataId;
                    *pBufLen = bufLen;
                    *dataBufAlloc = dataBuf;

                    // Keep some stats
                    if (takeStats) {
                        int64_t diff = 0;
                        int64_t droppedTicks = 0UL;
                        if (knowExpectedTick) {
                            diff = packetTick - expectedTick;
                            diff = (diff < 0) ? -diff : diff;
                            droppedTicks = diff / tickPrescale;

                            // In this case, it includes the discarded bufs (which it should not)
                            stats->droppedBuffers += droppedTicks; // estimate

                            // This works if all the buffers coming in are exactly the same size.
                            // If they're not, then the # of packets of this buffer
                            // is used to guess at how many packets were dropped for the dropped tick(s).
                            // Again, this includes discarded packets which it should not.
                            stats->droppedPackets += droppedTicks * pktCount;
                        }

                        stats->acceptedBytes    += totalBytesRead;
                        stats->acceptedPackets  += pktCount;

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
