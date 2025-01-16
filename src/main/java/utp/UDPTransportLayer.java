package utp;


import utp.data.UtpPacket;

import java.io.IOException;
import java.net.*;

import static utp.data.UtpPacketUtils.MAX_UDP_HEADER_LENGTH;
import static utp.data.UtpPacketUtils.MAX_UTP_PACKET_LENGTH;


public class UDPTransportLayer implements TransportLayer {

    protected DatagramSocket socket;
    private final Object sendLock = new Object();
    private DatagramSocket clientSocket;
    private InetSocketAddress serverSocketAddress;


    public UDPTransportLayer(String serverAddress, int serverPort) {
        try {
            this.socket = new DatagramSocket();
            this.serverSocketAddress = new InetSocketAddress(serverAddress, serverPort);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }

    public UDPTransportLayer(InetSocketAddress address) {
        try {
            this.socket = new DatagramSocket(address);
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void sendPacket(DatagramPacket packet) throws IOException {
        synchronized (sendLock) {
            packet.setAddress(serverSocketAddress.getAddress());
            packet.setPort(serverSocketAddress.getPort());
            this.socket.send(packet);
        }
    }

    @Override
    public void sendPacket(UtpPacket packet) throws IOException {
        sendPacket(UtpPacket.createDatagramPacket(packet));
    }

    @Override
    public DatagramPacket onPacketReceive() throws IOException {
        byte[] buffer = new byte[MAX_UDP_HEADER_LENGTH + MAX_UTP_PACKET_LENGTH];
        DatagramPacket dgpkt = new DatagramPacket(buffer, buffer.length);
        this.socket.receive(dgpkt);
        return dgpkt;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return this.serverSocketAddress;
    }

    @Override
    public void close() {
        this.socket.close();
    }
}
