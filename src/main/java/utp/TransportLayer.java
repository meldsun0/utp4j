package utp;

import utp.data.UtpPacket;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;

public interface TransportLayer {

    void sendPacket(DatagramPacket packet) throws IOException;

    void sendPacket(UtpPacket packet) throws IOException;

    DatagramPacket onPacketReceive() throws  IOException;

    SocketAddress getRemoteAddress();

    void close();
}
