package net.utp4j.channels.impl;

import net.utp4j.channels.UtpSocketState;
import net.utp4j.channels.futures.UtpWriteFuture;
import net.utp4j.channels.impl.read.UtpReadFutureImpl;
import net.utp4j.channels.impl.recieve.UtpPacketRecievable;
import net.utp4j.data.UtpPacket;
import net.utp4j.data.UtpPacketUtils;
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

public class UTPServer implements UtpPacketRecievable {

    private static final Logger LOG = LogManager.getLogger(UTPServer.class);

    private final Map<Integer, UTPClient> connectionIds = new HashMap<>();
    protected DatagramSocket socket;

    private AtomicBoolean listen = new AtomicBoolean(false);
    private final InetSocketAddress listenAddress;

    private final CompletableFuture<UTPClient> initAcceptanceFuture;

    public UTPServer(InetSocketAddress listenAddress) {
        this.listenAddress = listenAddress;
        this.initAcceptanceFuture = new CompletableFuture<>();
    }

    public void start() throws SocketException {
        LOG.info("Starting UTP server listening on {}", listenAddress);
        if (!listen.compareAndSet(false, true)) {
           // return null; //CompletableFuture.failedFuture(new IllegalStateException("Attempted to start an already started server listening on " + listenAddress));
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
                    recievePacket(dgpkt);
                } catch (IOException e) {
                    break;
                }
            }
        });
    }

    /*
     * handles syn packet.
     */
    private void synRecieved(DatagramPacket packet) {
        if (handleDoubleSyn(packet)) {
            return;
        }
        if (packet != null ) {
            try {
                UTPClient utpChannel =  UTPClient.open();
                utpChannel.setDgSocket(this.socket);
                utpChannel.recievePacket(packet);
                utpChannel.setServer(this);

                if (isChannelRegistrationNecessary(utpChannel)) {
                    connectionIds.put((int) (utpChannel.getConnectionIdRecieving() & 0xFFFF), utpChannel);
                    this.initAcceptanceFuture.complete(utpChannel);
                }else{
                    this.initAcceptanceFuture.completeExceptionally(new RuntimeException("Something went wrong!"));
                }

            } catch (IOException e) {
                this.initAcceptanceFuture.completeExceptionally(new RuntimeException("Something went wrong!"));
            }

        }
    }


    public UtpReadFutureImpl read(ByteBuffer dst) throws ExecutionException, InterruptedException {
        return this.initAcceptanceFuture.get().read(dst);
    }

    public void write(ByteBuffer dataToSend) throws ExecutionException, InterruptedException {
        this.initAcceptanceFuture.get().write(dataToSend);
    }

    /*
     * handles double syn....
     */
    private boolean handleDoubleSyn(DatagramPacket packet) {
        UtpPacket pkt = UtpPacketUtils.extractUtpPacket(packet);
        int connId = pkt.getConnectionId();
        connId = (connId & 0xFFFF) + 1;
        UTPClient client = connectionIds.get(connId);
        if (client != null) {
            client.recievePacket(packet);
            return true;
        }

        return false;
    }

    /*
     * handles the recieving of a pkt. if is a syn packet, the server takes care of it, otherwise it will be passed to the channel.
     */
    @Override
    public void recievePacket(DatagramPacket packet) {
        if (UtpPacketUtils.isSynPkt(packet)) {
            synRecieved(packet);
        } else {
            UtpPacket utpPacket = UtpPacketUtils.extractUtpPacket(packet);
            UTPClient client = connectionIds.get(utpPacket.getConnectionId() & 0xFFFF);
            client.recievePacket(packet);

        }
    }




    /*
     * true if channel reg. is required.
     */
    private boolean isChannelRegistrationNecessary(UTPClient channel) {
        return connectionIds.get(channel.getConnectionIdRecieving()) == null
                && channel.getState() != UtpSocketState.SYN_ACKING_FAILED;
    }


    public void close() throws ExecutionException, InterruptedException {
        if (listen.compareAndSet(true, false)) {
            LOG.info("Stopping UTP server listening on {}", listenAddress);
            if (connectionIds.isEmpty()) {
                socket.close();
                this.initAcceptanceFuture.get().close();
            }
        } else {
            LOG.warn("An attempt to stop already stopping/stopped UTP server");
        }
    }

    /**
     * Unregisters the channel.
     *
     * @param UTPClient
     */
    public void unregister(UTPClient UTPClient) {
        connectionIds.remove((int) UTPClient.getConnectionIdRecieving() & 0xFFFF);
    }


}
