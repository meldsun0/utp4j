package utp.message;

import utp.data.UtpPacket;
import static utp.data.UtpPacketUtils.FIN;
import static utp.data.bytes.UnsignedTypesUtil.longToUshort;

public class FinMessage {

    public static UtpPacket build(int timestamp, long connectionIdSending,
                                  int ackNumber, int sequenceNumber) {
        //TODO not use but do not forget to   this.currentSequenceNumber = Utils.incrementSeqNumber(this.currentSequenceNumber);
        return UtpPacket.builder()
                .typeVersion(FIN)
                .connectionId(longToUshort(connectionIdSending))
                .timestamp(timestamp)
                .ackNumber(longToUshort(ackNumber))
                .sequenceNumber(longToUshort(sequenceNumber))
                .build();
    }
}