package net.utp4j.channels.impl.recieve;

import java.net.DatagramPacket;


public interface UtpPacketRecievable {

    void recievePacket(DatagramPacket packet);

}
