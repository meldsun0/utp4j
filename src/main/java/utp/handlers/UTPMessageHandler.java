package utp.handlers;





import utp.Session;

import java.net.DatagramPacket;

public interface UTPMessageHandler<Message> {

    void handle(Session utpSession, DatagramPacket udpPacket);
}
