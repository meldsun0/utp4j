package net.utp4j.channels.impl;

import net.utp4j.channels.UtpSocketChannel;
import net.utp4j.channels.UtpSocketState;
import net.utp4j.channels.exception.CannotCloseServerException;
import net.utp4j.channels.futures.UtpAcceptFuture;
import net.utp4j.channels.impl.accept.UtpAcceptFutureImpl;
import net.utp4j.channels.impl.recieve.ConnectionIdTriplet;
import net.utp4j.channels.impl.recieve.UtpPacketRecievable;
import net.utp4j.channels.impl.recieve.UtpRecieveRunnable;
import net.utp4j.data.UtpPacket;
import net.utp4j.data.UtpPacketUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

public class UTPServer  implements UtpPacketRecievable {

    private static final Logger LOG = LogManager.getLogger(UTPServer.class);

    private UtpRecieveRunnable listenRunnable;
    private final Queue<UtpAcceptFutureImpl> acceptQueue = new LinkedList<UtpAcceptFutureImpl>();
    private final Map<Integer, ConnectionIdTriplet> connectionIds = new HashMap<Integer, ConnectionIdTriplet>();
    protected DatagramSocket socket;

    private AtomicBoolean listen = new AtomicBoolean(false);
    private final InetSocketAddress listenAddress;

    public UTPServer(InetSocketAddress listenAddress) {
        this.listenAddress = listenAddress;
    }


    public UtpAcceptFuture start() throws SocketException {
        LOG.info("Starting UTP server listening on {}", listenAddress);
        if (!listen.compareAndSet(false, true)) {
            return null ; //CompletableFuture.failedFuture(new IllegalStateException("Attempted to start an already started server listening on " + listenAddress));
        }
        this.socket = new DatagramSocket(this.listenAddress);
        this.listenRunnable = new UtpRecieveRunnable(this.socket, this);
        Thread listenRunner = new Thread(this.listenRunnable, "listenRunnable_" + this.socket.getLocalPort());
        listenRunner.start();

        UtpAcceptFutureImpl future;
        try {
            future = new UtpAcceptFutureImpl();
            acceptQueue.add(future);
            return future;
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;

    }

    /*
     * handles syn packet.
     */
    private void synRecieved(DatagramPacket packet) {
        if (handleDoubleSyn(packet)) {
            return;
        }
        if (packet != null && acceptQueue.peek() != null) {
            boolean registered = false;
            UtpAcceptFutureImpl future = acceptQueue.poll();
            UtpSocketChannelImpl utpChannel = null;
            try {
                utpChannel = (UtpSocketChannelImpl) UtpSocketChannel.open();
                utpChannel.setDgSocket(this.socket);
                utpChannel.recievePacket(packet);
                utpChannel.setServer(this);
                registered = registerChannel(utpChannel);
            } catch (IOException e) {
                future.setIOException(e);
            }

            /* Collision in Connection ids or failed to ack.
             * Ignore Syn Packet and let other side handle the issue. */
            if (!registered) {
                utpChannel = null;
            }
            future.synRecieved(utpChannel);
        }
    }

    /*
     * handles double syn....
     */
    private boolean handleDoubleSyn(DatagramPacket packet) {
        UtpPacket pkt = UtpPacketUtils.extractUtpPacket(packet);
        int connId = pkt.getConnectionId();
        connId = (connId & 0xFFFF) + 1;
        ConnectionIdTriplet triplet = connectionIds.get(connId);
        if (triplet != null) {
            triplet.getChannel().recievePacket(packet);
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
            ConnectionIdTriplet triplet = connectionIds.get(utpPacket.getConnectionId() & 0xFFFF);
            if (triplet != null) {
                triplet.getChannel().recievePacket(packet);
            }
        }
    }


    /*
     * registers a channel.
     */
    private boolean registerChannel(UtpSocketChannelImpl channel) {
        ConnectionIdTriplet triplet = new ConnectionIdTriplet(
                channel, channel.getConnectionIdRecieving(), channel.getConnectionIdsending());

        if (isChannelRegistrationNecessary(channel)) {
            connectionIds.put((int) (channel.getConnectionIdRecieving() & 0xFFFF), triplet);
            return true;
        }

        /* Connection id collision found or not been able to ack.
         *  ignore this syn packet */
        return false;
    }

    /*
     * true if channel reg. is required.
     */
    private boolean isChannelRegistrationNecessary(UtpSocketChannelImpl channel) {
        return connectionIds.get(channel.getConnectionIdRecieving()) == null
                && channel.getState() != UtpSocketState.SYN_ACKING_FAILED;
    }


    public void close() {
        if (listen.compareAndSet(true, false)) {
            LOG.info("Stopping UTP server listening on {}", listenAddress);
            if (connectionIds.isEmpty()) {
                    listenRunnable.graceFullInterrupt();

            }
        } else {
            LOG.warn("An attempt to stop already stopping/stopped UTP server");
        }
    }

    /**
     * Unregisters the channel.
     *
     * @param utpSocketChannelImpl
     */
    public void unregister(UtpSocketChannelImpl utpSocketChannelImpl) {
        connectionIds.remove((int) utpSocketChannelImpl.getConnectionIdRecieving() & 0xFFFF);
    }

}
