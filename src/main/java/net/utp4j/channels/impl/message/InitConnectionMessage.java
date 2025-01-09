package net.utp4j.channels.impl.message;

import net.utp4j.data.UtpPacket;

import static net.utp4j.data.UtpPacketUtils.SYN;
import static net.utp4j.data.bytes.UnsignedTypesUtil.longToUbyte;
import static net.utp4j.data.bytes.UnsignedTypesUtil.longToUshort;

public class InitConnectionMessage {

    public static UtpPacket build(int timestamp, long connectionId) {
        return UtpPacket.builder()
                .typeVersion(SYN)
                .sequenceNumber(longToUbyte(1))
                .payload(new byte[]{1, 2, 3, 4, 5, 6})
                .connectionId(longToUshort(connectionId))
                .timestamp(timestamp)
                .build();
    }
}
