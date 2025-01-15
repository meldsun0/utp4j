package utp.message;

import utp.data.UtpPacket;

import static utp.data.UtpPacketUtils.STATE;
import static utp.data.bytes.UnsignedTypesUtil.longToUint;
import static utp.data.bytes.UnsignedTypesUtil.longToUshort;

public class ACKMessage {

    public static UtpPacket build(int timeDifference, long advertisedWindow,
                                  int timestamp, long connectionIdSending,
                                  int ackNumber,
                                  byte firstExtension) {
        return UtpPacket.builder()
                .typeVersion(STATE)
                .firstExtension(firstExtension)
                .connectionId(longToUshort(connectionIdSending))
                .timestamp(timestamp)
                .timestampDifference(timeDifference)
                .windowSize(longToUint(advertisedWindow))
                .ackNumber(longToUshort(ackNumber))
                .build();
    }
}