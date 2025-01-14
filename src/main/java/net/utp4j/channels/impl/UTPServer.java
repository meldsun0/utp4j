package net.utp4j.channels.impl;

import net.utp4j.channels.SessionState;
import net.utp4j.channels.impl.channels.UTPChannel;
import net.utp4j.channels.impl.channels.UTPChannelListener;
import net.utp4j.channels.impl.channels.UTPServerChannel;
import net.utp4j.channels.impl.message.MessageType;
import net.utp4j.channels.impl.handlers.UTPWireMessageDecoder;
import net.utp4j.data.UtpPacket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.utp4j.data.UtpPacketUtils.MAX_UDP_HEADER_LENGTH;
import static net.utp4j.data.UtpPacketUtils.MAX_UTP_PACKET_LENGTH;

public class UTPServer implements UTPChannelListener {

    private static final Logger LOG = LogManager.getLogger(UTPServer.class);

    private final AtomicBoolean listen = new AtomicBoolean(false);
    private final InetSocketAddress listenAddress;

    private final CompletableFuture<UTPChannel> initAcceptanceFuture;
    private final Map<Integer, UTPChannel> connectionIds = new HashMap<>();
    private DatagramSocket socket;

    public UTPServer(InetSocketAddress listenAddress) {
        this.listenAddress = listenAddress;
        this.initAcceptanceFuture = new CompletableFuture<>();
    }

    public void start() throws SocketException {
        LOG.info("Starting UTP server listening on {}", listenAddress);
        if (!listen.compareAndSet(false, true)) {
            this.initAcceptanceFuture.completeExceptionally(new IllegalStateException("Attempted to start an already started server listening on " + listenAddress));
        }
        this.socket = new DatagramSocket(this.listenAddress);
        this.startReading();
    }


    private void startReading() {
        CompletableFuture.runAsync(() -> {
            while (listen.get()) {
                byte[] buffer = new byte[MAX_UDP_HEADER_LENGTH + MAX_UTP_PACKET_LENGTH];
                DatagramPacket dgpkt = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(dgpkt);
                    if (UTPWireMessageDecoder.decode(dgpkt).getMessageType() == MessageType.ST_SYN) {
                        synRecieved(dgpkt);




                    } else {
                        UtpPacket utpPacket = UtpPacket.decode(dgpkt);
                        UTPChannel client = connectionIds.get(utpPacket.getConnectionId() & 0xFFFF);
                        if (client != null) {
                            client.recievePacket(dgpkt);
                        }
                    }
                } catch (IOException e) {
                    break;
                }
            }
        });
    }


    private void synRecieved(DatagramPacket packet) {
        if (handleDoubleSyn(packet)) {
            return;
        }

        UTPServerChannel utpChannel = new   UTPServerChannel(this, new);
        utpChannel.recievePacket(packet);

        if (isChannelRegistrationNecessary(utpChannel)) {
            connectionIds.put((int) (utpChannel.getConnectionIdRecievingIncoming() & 0xFFFF), utpChannel);
        } else {
            utpChannel = null;
            this.initAcceptanceFuture.completeExceptionally(new RuntimeException("Something went wrong!"));
        }
        this.initAcceptanceFuture.complete(utpChannel);

    }

    public CompletableFuture<Void> read(ByteBuffer dst) throws ExecutionException, InterruptedException {
        return this.initAcceptanceFuture.get().read(dst);
    }

    public CompletableFuture<Void> write(ByteBuffer dataToSend) throws ExecutionException, InterruptedException {
        return this.initAcceptanceFuture.get().write(dataToSend);
    }

    private boolean handleDoubleSyn(DatagramPacket packet) {
        UtpPacket pkt = UtpPacket.decode(packet);
        int connId = pkt.getConnectionId();
        connId = (connId & 0xFFFF) + 1;
        UTPChannel client = connectionIds.get(connId);
        if (client != null) {
            client.recievePacket(packet);
            return true;
        }
        return false;
    }

    private boolean isChannelRegistrationNecessary(UTPChannel channel) {
        return connectionIds.get(channel.getConnectionIdRecievingIncoming()) == null
                && channel.getState() != SessionState.SYN_ACKING_FAILED;
    }

    public void stop() throws ExecutionException, InterruptedException {
        if (listen.compareAndSet(true, false)) {
            LOG.info("Stopping UTP server listening on {}", listenAddress);
            if (connectionIds.isEmpty()) {
                socket.close();
                this.initAcceptanceFuture.get().stop();
                //this.connectionIds.forEach();
            }
        } else {
            LOG.info("An attempt to stop already stopping/stopped UTP server");
        }
    }

    public void close(long connectionIdReceiving) {
        if (!listen.compareAndSet(true, false)) {
            LOG.warn("An attempt to stop an already stopping/stopped UTP server");
            return;
        }
        connectionIds.remove((int) connectionIdReceiving & 0xFFFF);

    }

}
