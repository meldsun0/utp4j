package net.utp4j.channels.impl;

import net.utp4j.channels.impl.channels.UTPChannel;
import net.utp4j.channels.impl.channels.UTPChannelListener;
import net.utp4j.channels.impl.channels.UTPClientChannel;
import net.utp4j.channels.impl.handlers.UTPIncomingRequestHandler;
import net.utp4j.data.util.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class UTPClient implements UTPChannelListener {

    private static final Logger LOG = LogManager.getLogger(UTPClient.class);
    private final UTPClientChannel utpClientChannel;


    private CompletableFuture<Void> incomingPacketFuture;
    private AtomicBoolean listen = new AtomicBoolean(false);

    private final UTPIncomingRequestHandler incomingRequestProcessor = new UTPIncomingRequestHandler();

    public UTPClient(InetSocketAddress address) {
        checkNotNull(address, "Address");
        this.utpClientChannel = new UTPClientChannel(this, new UDPTransportLayer(address));
    }

    public CompletableFuture<Void> connect(int connectionId) {
        checkArgument(Utils.isConnectionValid(connectionId), "ConnectionId invalid number");
        LOG.info("Starting UTP server listening on {}", connectionId);
        if (!listen.compareAndSet(false, true)) {
            CompletableFuture.failedFuture(new IllegalStateException("Attempted to start an already started server listening on " + connectionId));
        }

        startListeningIncomingPackets();
        return this.utpClientChannel.initConnection(connectionId);
    }

    private void startListeningIncomingPackets() {
        this.incomingPacketFuture = CompletableFuture.runAsync(() -> {
            while (listen.get()) {
                try {
                    this.utpClientChannel.recievePacket();
                } catch (IOException e) {
                    break;
                }
            }
        });
    }


    public void stop() {
        if (!listen.compareAndSet(true, false)) {
            LOG.warn("An attempt to stop an already stopping/stopped UTP server");
            return;
        }
        this.incomingPacketFuture.complete(null);
        this.utpClientChannel.stop();
    }

    @Override
    public void close(long connectionIdReceiving) {
        if (!listen.compareAndSet(true, false)) {
            LOG.warn("An attempt to stop an already stopping/stopped UTP server");
            return;
        }
        this.incomingPacketFuture.complete(null);
    }

    public CompletableFuture<Void> write(ByteBuffer buffer) {
        return this.utpClientChannel.write(buffer);
    }

    public CompletableFuture<Void> read(ByteBuffer buffer) {
        return this.utpClientChannel.read(buffer);
    }
}
