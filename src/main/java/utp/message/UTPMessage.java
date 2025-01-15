package utp.message;

import utp.data.UtpPacket;

public interface UTPMessage {

    UtpPacket build();
}
