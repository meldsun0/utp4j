package net.utp4j.channels.impl.channels;

import net.utp4j.channels.impl.TransportLayer;
import net.utp4j.channels.impl.UTPClient;
import net.utp4j.channels.impl.message.InitConnectionMessage;
import net.utp4j.channels.impl.message.MessageType;
import net.utp4j.data.UtpPacket;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public class UTPClientChannel extends UTPChannel {


    public UTPClientChannel(UTPChannelListener channelListener, TransportLayer transportLayer) {
        super(channelListener, transportLayer);
    }

    public CompletableFuture<Void> initConnection(int connectionId) {
        try {
            connection = new CompletableFuture<>();
            this.session.initConnection(connectionId);
            UtpPacket message = InitConnectionMessage.build(timeStamper.utpTimeStamp(), connectionId);
            this.transportLayer.sendPacket(message);
            this.session.updateStateOnConnectionInitSuccess();
            startConnectionTimeOutCounter(message);
            this.session.printState();
        } catch (IOException exp) {
            //TODO reconnect
        }
        return connection;
    }

    @Override
    public void recievePacket() throws IOException {
        {
            DatagramPacket udpPacket = this.transportLayer.onPacketReceive();
            //  MessageType messageType = UTPWireMessageDecoder.decode(udpPacket);
            UtpPacket utpPacket = UtpPacket.decode(udpPacket);
            MessageType messageType = utpPacket.getMessageType();
            switch (messageType) {
                case ST_RESET, ST_SYN -> this.stop();
                case ST_DATA, ST_STATE -> queuePacket(utpPacket, udpPacket);
                case ST_FIN -> handleFinPacket(utpPacket);
                default -> sendResetPacket(udpPacket.getSocketAddress());
            }
        }
    }
}
