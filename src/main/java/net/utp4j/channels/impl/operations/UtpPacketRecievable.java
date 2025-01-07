package net.utp4j.channels.impl.operations;

import java.net.DatagramPacket;


public interface UtpPacketRecievable {

    void recievePacket(DatagramPacket packet);

}
