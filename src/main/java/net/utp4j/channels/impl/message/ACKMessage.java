package net.utp4j.channels.impl.message;

import net.utp4j.data.UtpPacket;

import static net.utp4j.data.UtpPacketUtils.STATE;
import static net.utp4j.data.bytes.UnsignedTypesUtil.longToUint;
import static net.utp4j.data.bytes.UnsignedTypesUtil.longToUshort;

public class ACKMessage {

    public static UtpPacket build(int timeDifference, long advertisedWindow,
                                  int timestamp, long connectionIdSending,
                                  int ackNumber) {
        return UtpPacket.builder()
                .typeVersion(STATE)
                .connectionId(longToUshort(connectionIdSending))
                .timestamp(timestamp)
                .timestampDifference(timeDifference)
                .windowSize(longToUint(advertisedWindow))
                .ackNumber(longToUshort(ackNumber))
                .build();
    }
}