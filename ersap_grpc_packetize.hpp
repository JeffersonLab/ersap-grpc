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
 * Contains routines to create packets, fill them with data including 2 headers
 * that will direct it to and through a special FPGA router.
 * These packets will eventually be received at a given UDP destination equipped
 * to reassemble it with help of the 2nd, reassembly, header.<p>
 *
 * The main purpose of this header is to support the applications that simulate
 * an environment of sending data to a backend which will reassemble the data and
 * wait for a delay specified by the sender in order to simulate data processing.
 * This simulated backend will communicate to a Load Balancer's control plane,
 * or alternatively to a simulated CP. Note that the sender talks to the control
 * plane as well, telling it the most recent tick/event sent in the last second.
 */
#ifndef ERSAP_GRPC_PACKETIZE_H
#define ERSAP_GRPC_PACKETIZE_H



#include <iostream>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <cerrno>
#include <string>
#include <getopt.h>
#include <cinttypes>
#include <chrono>
#include <thread>
#include <system_error>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <net/if.h>

#ifdef __APPLE__
#include <cctype>
#endif




#define LB_HEADER_BYTES  16
#define HEADER_BYTES     36
#define RE_HEADER_BYTES  20


#ifdef __linux__
    #define htonll(x) ((1==htonl(1)) ? (x) : (((uint64_t)htonl((x) & 0xFFFFFFFFUL)) << 32) | htonl((uint32_t)((x) >> 32)))
    #define ntohll(x) ((1==ntohl(1)) ? (x) : (((uint64_t)ntohl((x) & 0xFFFFFFFFUL)) << 32) | ntohl((uint32_t)((x) >> 32)))
#endif


#ifndef _BYTESWAP_H
    #define _BYTESWAP_H

    static inline uint16_t bswap_16(uint16_t x) {
        return (x>>8) | (x<<8);
    }

    static inline uint32_t bswap_32(uint32_t x) {
        return (bswap_16(x&0xffff)<<16) | (bswap_16(x>>16));
    }

    static inline uint64_t bswap_64(uint64_t x) {
        return (((uint64_t)bswap_32(x&0xffffffffull))<<32) |
                          (bswap_32(x>>32));
    }
#endif

#define btoa(x) ((x)?"true":"false")
#define INPUT_LENGTH_MAX 256



namespace ejfat {

    static int getMTU(const char *interfaceName, bool debug) {
        // Default MTU
        int mtu = 1500;

        int sock = socket(PF_INET, SOCK_DGRAM, IPPROTO_IP);
        struct ifreq ifr;
        strcpy(ifr.ifr_name, interfaceName);
        if (!ioctl(sock, SIOCGIFMTU, &ifr)) {
            mtu = ifr.ifr_mtu;
            if (debug) fprintf(stderr, "ioctl says MTU = %d\n", mtu);
        } else {
            if (debug) fprintf(stderr, "cannot find MTU, try %d\n", mtu);
        }
        close(sock);
        return mtu;
    }


    /**
     * Attempt to set the MTU value for UDP packets on the given interface.
     * Miminum 500, maximum 9000.
     *
     * @param interfaceName name of network interface (e.g. eth0).
     * @param sock UDP socket on which to set mtu value.
     * @param mtu the successfully set mtu value or -1 if could not be set.
     * @param debug true for debug output.
     * @return
     */
    static int setMTU(const char *interfaceName, int sock, int mtu, bool debug) {

        if (mtu < 500) {
            mtu = 500;
        }
        if (mtu > 9000) {
            mtu = 9000;
        }

        struct ifreq ifr;
        strcpy(ifr.ifr_name, interfaceName);
        ifr.ifr_mtu = mtu;

        if (!ioctl(sock, SIOCSIFMTU, &ifr)) {
            // Mtu changed successfully
            mtu = ifr.ifr_mtu;
            if (debug) fprintf(stderr, "set MTU to %d\n", mtu);
        } else {
            if (!ioctl(sock, SIOCGIFMTU, &ifr)) {
                mtu = ifr.ifr_mtu;
                if (debug) fprintf(stderr, "Failed to set mtu, using default = %d\n", mtu);
            } else {
                if (debug) fprintf(stderr, "Using default MTU\n");
                return -1;
            }
        }

#ifdef __linux__
        // For jumbo (> 1500 B) frames we need to set the "no fragment" flag.
        // Only possible on linux, not mac.
        if (mtu > 1500) {
            int val = IP_PMTUDISC_DO;
            setsockopt(sock, IPPROTO_IP, IP_MTU_DISCOVER, &val, sizeof(val));
        }
#endif

        return mtu;
    }


    /**
     * Set the Load Balancer header data.
     * The first four bytes go as ordered.
     * The entropy goes as a single, network byte ordered, 16-bit int.
     * The tick goes as a single, network byte ordered, 64-bit int.
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
     *  |              Rsvd             |            Entropy            |
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
     * @param buffer   buffer in which to write the header.
     * @param tick     unsigned 64 bit tick number used to tell the load balancer
     *                 which backend host to direct the packet to.
     * @param version  version of this software.
     * @param protocol protocol this software uses.
     * @param entropy  entropy field used to determine destination port.
     */
    static void setLbMetadata(char *buffer, uint64_t tick, int version, int protocol, int entropy) {
        *buffer = 'L';
        *(buffer + 1) = 'B';
        *(buffer + 2) = version;
        *(buffer + 3) = protocol;
        // Put the data in network byte order (big endian)
        *((uint16_t * )(buffer + 6)) = htons(entropy);
        *((uint64_t * )(buffer + 8)) = htonll(tick);
    }


    /**
     * <p>Set the Reassembly Header data.
     * The first 16 bits go as ordered. The dataId is put in network byte order.
     * The offset, length and tick are also put into network byte order.</p>
     * Implemented <b>without</b> using C++ bit fields.
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
     * @param buffer  buffer in which to write the header.
     * @param offset  byte offset into full buffer payload.
     * @param length  total length in bytes of full buffer payload.
     * @param tick    64 bit tick number used to tell the load balancer
     *                which backend host to direct the packet to. Necessary to
     *                disentangle packets from different ticks at one destination
     *                as there may be overlap in time.
     * @param version the version of this software.
     * @param dataId  the data source id number.
     */
    static void setReMetadata(char *buffer, uint32_t offset, uint32_t length,
                              uint64_t tick, int version, uint16_t dataId) {

        buffer[0] = version << 4;

        *((uint16_t * )(buffer + 2)) = htons(dataId);
        *((uint32_t * )(buffer + 4)) = htonl(offset);
        *((uint32_t * )(buffer + 8)) = htonl(length);
        *((uint64_t * )(buffer + 12)) = htonll(tick);
    }


    /**
     * <p>
     * Set the data for a synchronization message sent directly to the load balancer.
     * The first 3 fields go as ordered. The srcId, evtNum, evtRate and time are all
     * put into network byte order.</p>
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
     * @param buffer   buffer in which to write the data.
     * @param version  version of this software.
     * @param srcId    id number of this data source.
     * @param evtNum   unsigned 64 bit event number used to tell the load balancer
     *                 which backend host to direct the packet to. This message
     *                 is telling the load balancer that this application has
     *                 already sent this, latest, event.
     * @param evtRate  in Hz, the rate this application is sending events
     *                 to the load balancer (0 if unknown).
     * @param nanos    at what unix time in nanoseconds was this message sent (0 if unknown).
     */
    static void setSyncData(char *buffer, int version, uint32_t srcId,
                            uint64_t evtNum, uint32_t evtRate, uint64_t nanos) {
        buffer[0] = 'L';
        buffer[1] = 'C';
        buffer[2] = version;

        // Put the data in network byte order (big endian)
        *((uint32_t * )(buffer + 4)) = htonl(srcId);
        *((uint64_t * )(buffer + 8)) = htonll(evtNum);
        *((uint32_t * )(buffer + 16)) = htonl(evtRate);
        *((uint64_t * )(buffer + 20)) = htonll(nanos);
    }


    /**
    * This method prints out the desired number of data bytes starting from the given index
    * without regard to the limit.
    *
    * @param buf     data to pring
    * @param bytes   number of bytes to print in hex
    * @param label   a label to print as header
    */
    static void printPktData(char *buf, size_t bytes, std::string const &label) {

        std::cout << label << ":" << std::endl;

        for (size_t i = 0; i < bytes; i++) {
            if (i % 20 == 0) {
                std::cout << "\n  array[" << (i + 1) << "-" << (i + 20) << "] =  ";
            } else if (i % 4 == 0) {
                std::cout << "  ";
            }

            printf("%02x ", (char) (buf[i]));
        }
        std::cout << std::endl << std::endl;
    }


    /**
     * <p>
     * This routine uses the latest, 20-byte RE header.
     * Generate data and send to a given destination by UDP.
     * The receiver is responsible for reassembling these packets back into the original data.</p>
     * <p>
     * Create the necessary # of packets and fill each with data.
     * In each pkt we send the:
     * <ol>
     * <li>backend "processing time" in millisec (4 bytes).
     * <li>total number of pkts for this buffer
     * <li>packet sequence (1,2,3, ...)
     * <li>junk data
     * </ol>
     * </p>
     * This routine calls "send" on a connected socket.
     * All data (header and actual data from dataBuffer arg) are copied into a separate
     * buffer and sent. The original data is unchanged.
     * This uses the new, version 2, RE header.
     *
     * @param dataLen        number of data bytes to be sent.
     * @param maxUdpPayload  maximum number of bytes to place into one UDP packet.
     * @param backendTime    time in milliseconds for backend to simulate processing of data from this buffer.
     * @param clientSocket   UDP sending socket.
     *
     * @param tick           value used by load balancer in directing packets to final host.
     * @param protocol       protocol in laad balance header.
     * @param entropy        entropy in laad balance header.
     * @param version        version in reassembly header.
     * @param dataId         data id in reassembly header.
     *
     * @param delay          delay in microsec between each packet being sent.
     * @param delayPrescale  prescale for delay (i.e. only delay every Nth time).
     * @param delayCounter   value-result parameter tracking when delay was last run.
     * @param debug          turn debug printout on & off.
     * @param packetsSent    filled with number of packets sent over network (valid even if error returned).
     *
     * @return 0 if OK, -1 if error when sending packet. Use errno for more details.
     */
    static int sendPacketizedBuf(uint32_t dataLen, int maxUdpPayload, uint32_t backendTime,
                                 int clientSocket, uint64_t tick, int protocol, int entropy,
                                 int version, uint16_t dataId,
                                 uint32_t delay, uint32_t delayPrescale, uint32_t *delayCounter,
                                 bool debug, int64_t *packetsSent) {

        uint32_t bytesToWrite = dataLen;
        uint32_t remainingBytes = dataLen;


        // Offset for the packet currently being sent (into full buffer)
        uint32_t localOffset = 0;
        uint32_t packetCounter = 0;
        // Round up to find total # packets to send maxUdpPayload's worth of data (not including all headers)
        uint32_t totalPackets = (dataLen + maxUdpPayload - 1) / maxUdpPayload;
        uint32_t remainingPackets = totalPackets;


        // Allocate something that'll hold one jumbo packet.
        char buffer[10000];

        // Write LB meta data into buffer - same for each packet so write once
        setLbMetadata(buffer, tick, version, protocol, entropy);

        // This is where we write data
        uint32_t *data = (uint32_t * )(buffer + HEADER_BYTES);

        // Write data that does not change only once
        if (debug) fprintf(stderr, "Send %u backend time\n", backendTime);
        data[0] = htonl(backendTime);
        data[1] = htonl(totalPackets);


        while (remainingPackets-- > 0) {
            // The number of regular data bytes comprising this packet
            bytesToWrite = remainingBytes > maxUdpPayload ? maxUdpPayload : remainingBytes;

            // Write RE meta data into buffer (in which offset differs for each packet)
            setReMetadata(buffer + LB_HEADER_BYTES, localOffset, dataLen, tick, version, dataId);

            // Write data that changes with each packet
            data[2] = htonl(++packetCounter);

            // Send packet to receiver
            if (debug) fprintf(stderr, "Send %u bytes\n", bytesToWrite);

            int err = send(clientSocket, buffer, bytesToWrite + HEADER_BYTES, 0);
            if (err == -1) {
                *packetsSent = totalPackets - remainingPackets - 1;
                perror(nullptr);
                return (-1);
            }

            if (err != (bytesToWrite + HEADER_BYTES)) {
                fprintf(stderr, "sendPacketizedBufferSend: wanted to send %d, but only sent %d\n",
                        (int) (bytesToWrite + HEADER_BYTES), err);
            }

            // delay if any
            if (delay > 0) {
                if (--(*delayCounter) < 1) {
                    std::this_thread::sleep_for(std::chrono::microseconds(delay));
                    *delayCounter = delayPrescale;
                }
            }

            localOffset += bytesToWrite;
            remainingBytes -= bytesToWrite;

            if (debug)
                fprintf(stderr, "Sent pkt %u, remaining %u bytes\n\n",
                        packetCounter, remainingBytes);
        }

        *packetsSent = totalPackets;

        return 0;
    }

}


#endif // ERSAP_GRPC_PACKETIZE_H
