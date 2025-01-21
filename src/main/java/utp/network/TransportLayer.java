package utp.network;

import utp.data.UtpPacket;
import utp.network.udp.UDPAddress;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;

public interface TransportLayer<T extends  TransportAddress> {

    void sendPacket(UtpPacket packet, T remoteAddress) throws IOException;

    UtpPacket onPacketReceive() throws  IOException;

    T getRemoteAddress();

    void close();
}
