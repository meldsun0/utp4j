package utp.message;

import utp.data.UtpPacket;

import static utp.data.UtpPacketUtils.SYN;
import static utp.data.bytes.UnsignedTypesUtil.longToUbyte;
import static utp.data.bytes.UnsignedTypesUtil.longToUshort;

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
