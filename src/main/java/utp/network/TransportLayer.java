package utp.network;

import utp.data.UtpPacket;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;

public interface TransportLayer {

    void sendPacket(UtpPacket packet) throws IOException;

    UtpPacket onPacketReceive() throws  IOException;

    SocketAddress getRemoteAddress();

    void close();
}
