package net.utp4j.channels.impl;

import net.utp4j.data.UtpPacket;

import java.io.IOException;
import java.net.DatagramPacket;

import java.net.SocketAddress;

public interface TransportLayer<E> {

    void sendPacket(DatagramPacket packet) throws IOException;

    void sendPacket(UtpPacket packet) throws IOException;

    DatagramPacket onPacketReceive() throws  IOException;

    SocketAddress getRemoteAddress();

    void close();
}
