/* Copyright 2013 Ivan Iljkic
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.utp4j.data;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;

import static net.utp4j.data.bytes.UnsignedTypesUtil.longToUbyte;


/**
 * Helper methods for uTP Headers.
 *
 * @author Ivan Iljkic (i.iljkic@gmail.com)
 */
public class UtpPacketUtils {

    public static final byte VERSION = longToUbyte(1);

    public static final byte DATA = (byte) (VERSION | longToUbyte(0));
    public static final byte FIN = (byte) (VERSION | longToUbyte(16));
    public static final byte STATE = (byte) (VERSION | longToUbyte(32));
    public static final byte RESET = (byte) (VERSION | longToUbyte(48));
    public static final byte SYN = (byte) (VERSION | longToUbyte(64));


    public static final byte NO_EXTENSION = longToUbyte(0);
    public static final Bytes SELECTIVE_ACK = Bytes.of(01); ;

    public static final int MAX_UTP_PACKET_LENGTH = 1500;
    public static final int MAX_UDP_HEADER_LENGTH = 48;
    public static final int DEF_HEADER_LENGTH = 20;

    private static final Logger log = LoggerFactory.getLogger(UtpPacketUtils.class);


    /**
     * Creates an Utp-Packet to initialize a connection
     * Following values will be set:
     * <ul>
     * <li>Type and Version</li>
     * <li>Sequence Number</li>
     * </ul>
     *
     * @return {@link UtpPacket}
     */
    public static UtpPacket createSynPacket(Bytes connectionId, UInt32 utpTimestamp ) {
        UtpPacket pkt = new UtpPacket();
        pkt.setTypeVersion(SYN);
        pkt.setSequenceNumber(UInt32.valueOf(1).toBytes());
        byte[] pl = {1, 2, 3, 4, 5, 6};
        pkt.setPayload(Bytes.of(pl));
        pkt.setConnectionId(connectionId);
        pkt.setTimestamp(utpTimestamp);
        return pkt;
    }

    public static UtpPacket extractUtpPacket(DatagramPacket dgpkt) {
        UtpPacket pkt = new UtpPacket();
        byte[] pktb = dgpkt.getData();
        pkt.setFromByteArray(pktb, dgpkt.getLength(), dgpkt.getOffset());
        return pkt;
    }

    public static boolean isSynPkt(UtpPacket packet) {

        if (packet == null) {
            return false;
        }

        return packet.getTypeVersion() == SYN;

    }


    private static boolean isPacketType(DatagramPacket packet, byte flag) {
        if (packet == null) {
            return false;
        }

        byte[] data = packet.getData();

        if (data != null && data.length >= DEF_HEADER_LENGTH) {
            return data[0] == flag;
        }

        return false;
    }

    public static boolean isSynPkt(DatagramPacket packet) {
        return isPacketType(packet, SYN);
    }

    public static boolean isResetPacket(DatagramPacket udpPacket) {
        return isPacketType(udpPacket, RESET);
    }

    public static boolean isDataPacket(DatagramPacket udpPacket) {
        return isPacketType(udpPacket, DATA);
    }

    public static boolean isStatePacket(DatagramPacket udpPacket) {
        return isPacketType(udpPacket, STATE);
    }

    public static boolean isFinPacket(DatagramPacket udpPacket) {
        return isPacketType(udpPacket, FIN);
    }

}
