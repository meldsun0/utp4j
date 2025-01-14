package net.utp4j.channels.impl.channels;

import net.utp4j.channels.impl.TransportLayer;
import net.utp4j.channels.impl.alg.UtpAlgConfiguration;
import net.utp4j.channels.impl.message.ACKMessage;
import net.utp4j.channels.impl.message.MessageType;
import net.utp4j.data.UtpPacket;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import static net.utp4j.channels.SessionState.CLOSED;
import static net.utp4j.channels.SessionState.CONNECTED;
import static net.utp4j.data.UtpPacketUtils.NO_EXTENSION;

public class UTPServerChannel extends UTPChannel {


    public UTPServerChannel(UTPChannelListener channelListener, TransportLayer transportLayer) {
        super(channelListener, transportLayer);
    }

    @Override
    public CompletableFuture<Void> initConnection(int connectionId) {
        return CompletableFuture.completedFuture(null);

    }

    @Override
    public void recievePacket() throws IOException {
        {
            DatagramPacket udpPacket = this.transportLayer.onPacketReceive();
            //  MessageType messageType = UTPWireMessageDecoder.decode(udpPacket);
            UtpPacket utpPacket = UtpPacket.decode(udpPacket);
            MessageType messageType = utpPacket.getMessageType();
            switch (messageType) {
                case ST_RESET -> this.stop();
                case ST_SYN -> handleIncommingConnectionRequest(utpPacket, udpPacket.getSocketAddress());
                case ST_DATA, ST_STATE -> queuePacket(utpPacket, udpPacket);
                case ST_FIN -> handleFinPacket(utpPacket);
                default -> sendResetPacket(udpPacket.getSocketAddress());
            }
        }
    }

    private void  handleIncommingConnectionRequest(UtpPacket utpPacket, SocketAddress socketAddress) {
        if (this.session.getState() == CLOSED ||
                (this.session.getState() == CONNECTED && isSameAddressAndId(utpPacket.getConnectionId(), socketAddress))) {
            try {
                this.session.initServerConnection(utpPacket.getConnectionId(), utpPacket.getSequenceNumber());
                session.printState();
                int timestampDifference = timeStamper.utpDifference(timeStamper.utpTimeStamp(), utpPacket.getTimestamp());
                //TODO validate that the seq number is sent!
                UtpPacket packet = ACKMessage.build(timestampDifference, UtpAlgConfiguration.MAX_PACKET_SIZE * 1000L, timeStamper.utpTimeStamp(), this.session.getConnectionIdSending(), this.session.getAckNumber(), NO_EXTENSION);
                this.transportLayer.sendPacket(packet);
                this.session.changeState(CONNECTED);
            } catch (IOException exp) {
                // TODO:
                this.session.syncAckFailed();
                exp.printStackTrace();
            }
        } else {
            sendResetPacket(socketAddress);
        }
    }


}
