package net.utp4j.channels.impl;

import net.utp4j.channels.UtpSocketState;
import net.utp4j.channels.impl.message.MessageType;
import net.utp4j.channels.impl.message.UTPWireMessageDecoder;
import net.utp4j.data.UtpPacket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static net.utp4j.data.UtpPacketUtils.MAX_UDP_HEADER_LENGTH;
import static net.utp4j.data.UtpPacketUtils.MAX_UTP_PACKET_LENGTH;

public class UTPServer {

    private static final Logger LOG = LogManager.getLogger(UTPServer.class);
    private final AtomicBoolean listen = new AtomicBoolean(false);
    private final InetSocketAddress listenAddress;

    private final CompletableFuture<UTPClient> initAcceptanceFuture;

    protected DatagramSocket socket;
    private final Map<Integer, UTPClient> connectionIds = new HashMap<>();

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
                    handleIncomingPacketOnServer(dgpkt);
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

        UTPClient utpChannel = new UTPClient(this.socket, this);
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
        UTPClient client = connectionIds.get(connId);
        if (client != null) {
            client.recievePacket(packet);
            return true;
        }
        return false;
    }

    public void handleIncomingPacketOnServer(DatagramPacket packet) {
        if (UTPWireMessageDecoder.decode(packet) == MessageType.ST_SYN) {
            synRecieved(packet);
        } else {
            UtpPacket utpPacket = UtpPacket.decode(packet);
            UTPClient client = connectionIds.get(utpPacket.getConnectionId() & 0xFFFF);
            if (client != null) {
                client.recievePacket(packet);
            }
        }
    }

    private boolean isChannelRegistrationNecessary(UTPClient channel) {
        return connectionIds.get(channel.getConnectionIdRecievingIncoming()) == null
                && channel.getState() != UtpSocketState.SYN_ACKING_FAILED;
    }

    public void close() throws ExecutionException, InterruptedException {
        if (listen.compareAndSet(true, false)) {
            LOG.info("Stopping UTP server listening on {}", listenAddress);
            if (connectionIds.isEmpty()) {
                socket.close();
                this.initAcceptanceFuture.get().stop();
            }
        } else {
            LOG.info("An attempt to stop already stopping/stopped UTP server");
        }
    }

    public void unregister(UTPClient UTPClient) {
        connectionIds.remove((int) UTPClient.getConnectionIdRecievingIncoming() & 0xFFFF);
    }
}
