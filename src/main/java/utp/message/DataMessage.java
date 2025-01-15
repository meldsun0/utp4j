package utp.message;

import utp.data.UtpPacket;
import static utp.data.UtpPacketUtils.DATA;
import static utp.data.bytes.UnsignedTypesUtil.longToUshort;

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
