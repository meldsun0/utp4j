package utp.network.udp;


import utp.data.UtpPacket;
import utp.message.UTPWireMessageDecoder;
import utp.network.TransportAddress;
import utp.network.TransportLayer;

import java.io.IOException;
import java.net.*;

import static utp.data.UtpPacketUtils.MAX_UDP_HEADER_LENGTH;
import static utp.data.UtpPacketUtils.MAX_UTP_PACKET_LENGTH;


public class UDPTransportLayer implements TransportLayer<UDPAddress> {

    protected DatagramSocket socket;
    private final Object sendLock = new Object();
    private UDPAddress remoteAddress;


    public UDPTransportLayer(String serverAddress, int serverPort) {
        try {
            this.socket = new DatagramSocket();
            this.remoteAddress = new UDPAddress(serverAddress, serverPort);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sendPacket(UtpPacket packet, UDPAddress remoteAddress) throws IOException {
        synchronized (sendLock) {
            DatagramPacket UDPPacket = UtpPacket.createDatagramPacket(packet);
            UDPPacket.setAddress(remoteAddress.getAddress());
            UDPPacket.setPort(remoteAddress.getPort());
            this.socket.send(UDPPacket);
        }
    }


    @Override
    public UtpPacket onPacketReceive() throws IOException {
        byte[] buffer = new byte[MAX_UDP_HEADER_LENGTH + MAX_UTP_PACKET_LENGTH];
        DatagramPacket dgpkt = new DatagramPacket(buffer, buffer.length);
        this.socket.receive(dgpkt);
        return UTPWireMessageDecoder.decode(dgpkt);
    }

    @Override
    public UDPAddress getRemoteAddress() {
        return this.remoteAddress;
    }

    @Override
    public void close() {
        this.socket.close();
    }
}
