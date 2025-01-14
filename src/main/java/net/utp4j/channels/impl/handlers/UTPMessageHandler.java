package net.utp4j.channels.impl.handlers;



import net.utp4j.channels.impl.Session;

import java.net.DatagramPacket;

public interface UTPMessageHandler<Message> {

    void handle(Session utpSession, DatagramPacket udpPacket);
}
