package net.utp4j.channels.impl.message;

import net.utp4j.data.UtpPacket;
import static net.utp4j.data.UtpPacketUtils.DATA;
import static net.utp4j.data.bytes.UnsignedTypesUtil.longToUshort;

public class DataMessage {

    public static UtpPacket build(int timestamp, long connectionIdSending,
                                  int ackNumber, int sequenceNumber) {
        return UtpPacket.builder()
                .typeVersion(DATA)
                .connectionId(longToUshort(connectionIdSending))
                .timestamp(timestamp)
                .ackNumber(longToUshort(ackNumber))
                .sequenceNumber(longToUshort(sequenceNumber))
                .build();
    }

}
