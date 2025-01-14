package net.utp4j.channels.impl;

import net.utp4j.data.UtpPacket;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.CompletableFuture;

import static net.utp4j.data.UtpPacketUtils.MAX_UDP_HEADER_LENGTH;
import static net.utp4j.data.UtpPacketUtils.MAX_UTP_PACKET_LENGTH;

public class UDPTransportLayer implements TransportLayer {

    protected DatagramSocket socket;
    private final Object sendLock = new Object();

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
            this.socket.send(packet);
        }
    }

    @Override
    public void sendPacket(UtpPacket packet) throws IOException {
        sendPacket(UtpPacket.createDatagramPacket(packet, socket.getRemoteSocketAddress()));
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
        return this.socket.getRemoteSocketAddress();
    }

    @Override
    public void close() {
        this.socket.close();
    }
}
