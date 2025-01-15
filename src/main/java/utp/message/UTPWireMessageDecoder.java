package utp.message;


import java.net.DatagramPacket;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class UTPWireMessageDecoder {

    public static final int DEF_HEADER_LENGTH = 20;

    public static MessageType decode(DatagramPacket udpPacket) {
        checkNotNull(udpPacket, "UDPPacket could not be null when decoding a UTP Wire Message");
        checkNotNull(udpPacket.getData(), "UDPPacket should have data");
        checkArgument(udpPacket.getData().length >= DEF_HEADER_LENGTH, "UDPPacket data should have more than 1 bytes when decoding a UTP Message");

        byte packetType = udpPacket.getData()[0];
        MessageType messageType = MessageType.fromByte(packetType);
        checkNotNull(messageType, "Invalid message type from nibble: " + packetType);

        return messageType;
    }
}
