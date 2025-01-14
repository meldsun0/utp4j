package net.utp4j.channels.impl.channels;

import net.utp4j.channels.SessionState;
import net.utp4j.channels.impl.Session;
import net.utp4j.channels.impl.TransportLayer;
import net.utp4j.channels.impl.UtpTimestampedPacketDTO;
import net.utp4j.channels.impl.alg.UtpAlgConfiguration;
import net.utp4j.channels.impl.message.*;
import net.utp4j.channels.impl.operations.UTPReadingFuture;
import net.utp4j.channels.impl.operations.UTPWritingFuture;
import net.utp4j.data.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;

import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static net.utp4j.channels.SessionState.*;
import static net.utp4j.data.UtpPacketUtils.*;


public abstract class UTPChannel {

    private static final Logger LOG = LogManager.getLogger(UTPChannel.class);
    private int connectionIdMASK = 0xFFFF;

    protected Session session;

    private final BlockingQueue<UtpTimestampedPacketDTO> queue = new LinkedBlockingQueue<UtpTimestampedPacketDTO>();


    private ScheduledExecutorService retryConnectionTimeScheduler;

    private Optional<UTPWritingFuture> writer = Optional.empty();
    private Optional<UTPReadingFuture> reader = Optional.empty();

    protected CompletableFuture<Void> connection;
    protected final UTPChannelListener channelListener;
    protected final TransportLayer transportLayer;
    protected MicroSecondsTimeStamp timeStamper = new MicroSecondsTimeStamp();


    public UTPChannel(final UTPChannelListener channelListener, final TransportLayer transportLayer) {
        this.session = new Session();
        this.channelListener = channelListener;
        this.transportLayer = transportLayer;
    }

    public abstract CompletableFuture<Void> initConnection(int connectionId);

    public abstract void recievePacket() throws IOException;




    protected void handleFinPacket(UtpPacket packet) {
        try {
            this.session.changeState(GOT_FIN);
            long freeBuffer = (reader.isPresent() && reader.get().isAlive()) ? reader.get().getLeftSpaceInBuffer() : UtpAlgConfiguration.MAX_PACKET_SIZE;
            UtpPacket ackPacket = buildACKPacket(packet, timeStamper.utpDifference(packet.getTimestamp()), freeBuffer);
            this.transportLayer.sendPacket(ackPacket);
        } catch (IOException e) {
            //TODO error when sending ack
        }

    }

    protected void handleConfirmationOfConnection(UtpPacket utpPacket, SocketAddress socketAddress) {
        if ((utpPacket.getConnectionId() & connectionIdMASK) == this.session.getConnectionIdReceiving()) {
            this.session.connectionConfirmed(utpPacket.getSequenceNumber());
            disableConnectionTimeOutCounter();
            connection.complete(null);
            this.session.printState();
        } else {
            sendResetPacket(socketAddress);
        }
    }


    protected boolean isSameAddressAndId(long connectionId, SocketAddress addr) {
        return (connectionId & 0xFFFFFFFF) == getConnectionIdRecievingIncoming() && addr.equals(this.transportLayer.getRemoteAddress());
    }

    protected void disableConnectionTimeOutCounter() {
        if (retryConnectionTimeScheduler != null) {
            retryConnectionTimeScheduler.shutdown();
            retryConnectionTimeScheduler = null;
        }
        this.session.resetConnectionAttempts();
    }

    protected void queuePacket(UtpPacket utpPacket, DatagramPacket udpPacket) {
        if ( utpPacket.getMessageType() == MessageType.ST_STATE && this.session.getState() == SessionState.SYN_SENT) {
            handleConfirmationOfConnection(utpPacket, udpPacket.getSocketAddress());
            return;
        }
        queue.offer(new UtpTimestampedPacketDTO(udpPacket, utpPacket, timeStamper.timeStamp(), timeStamper.utpTimeStamp()));
    }

    protected void sendResetPacket(SocketAddress addr) {
        //TODO
        LOG.debug("Sending RST packet MUST BE IMPLEMENTED");

    }


    public CompletableFuture<Void> write(ByteBuffer buffer) {
        return this.connection.whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        return;
                    }
                    this.writer = Optional.of(new UTPWritingFuture(this, buffer, timeStamper));
                })
                .thenCompose(ignored -> {
                    if (writer.isPresent()) {
                        return writer.get().startWriting();
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public CompletableFuture<Void> read(ByteBuffer dst) {
        return this.connection.whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        return;
                    }
                    this.reader = Optional.of(new UTPReadingFuture(this, dst, timeStamper));
                })
                .thenCompose(ignored -> {
                    if (reader.isPresent()) {
                        return reader.get().startReading();
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    public BlockingQueue<UtpTimestampedPacketDTO> getQueue() {
        return queue;
    }

    public void stop() {
        this.session.close();
        this.reader.ifPresent(UTPReadingFuture::graceFullInterrupt);
        this.writer.ifPresent(UTPWritingFuture::graceFullInterrupt);
        this.transportLayer.close();
        this.channelListener.close(this.session.getConnectionIdReceiving());
    }


    public UtpPacket buildSelectiveACK(SelectiveAckHeaderExtension extension, int timestampDifference, long windowSize, byte firstExtension) {
        SelectiveAckHeaderExtension[] extensions = {extension};
        UtpPacket packet = ACKMessage.build(timestampDifference, windowSize, timeStamper.utpTimeStamp(),
                this.session.getConnectionIdSending(), this.session.getAckNumber(), firstExtension);
        packet.setExtensions(extensions);
        return packet;
    }

    public UtpPacket buildDataPacket() {
        UtpPacket utpPacket = DataMessage.build(timeStamper.utpTimeStamp(),
                this.session.getConnectionIdSending(), this.session.getAckNumber(), this.session.getSequenceNumber());
        this.session.incrementeSeqNumber();
        return utpPacket;
    }

    public UtpPacket buildACKPacket(UtpPacket utpPacket, int timestampDifference, long windowSize) throws IOException {
        if (utpPacket.getTypeVersion() != FIN) {
            this.session.updateAckNumber(utpPacket.getSequenceNumber());
        }
        //TODO validate that the seq number is sent!
        return ACKMessage.build(timestampDifference, windowSize, this.timeStamper.utpTimeStamp(),
                this.session.getConnectionIdSending(), this.getAckNumber(), NO_EXTENSION);
    }

    public void returnFromReading() {
        //TODO: dispatch:
        this.writer.ifPresent(writer -> {
            if (writer.isAlive()) this.session.changeState(SessionState.CONNECTED);
        });
    }

    protected void startConnectionTimeOutCounter(UtpPacket synPacket) {
        retryConnectionTimeScheduler = Executors.newSingleThreadScheduledExecutor();
        retryConnectionTimeScheduler.scheduleWithFixedDelay(() -> {
                    this.resendSynPacket(synPacket);
                },
                UtpAlgConfiguration.CONNECTION_ATTEMPT_INTERVALL_MILLIS,
                UtpAlgConfiguration.CONNECTION_ATTEMPT_INTERVALL_MILLIS,
                TimeUnit.MILLISECONDS);
    }


    public void resendSynPacket(UtpPacket synPacket) {
        int attempts = this.session.getConnectionAttempts();
        if (this.session.getState() != SessionState.SYN_SENT) {
            return;
        }
        if (attempts >= UtpAlgConfiguration.MAX_CONNECTION_ATTEMPTS) {
            this.connection.completeExceptionally(new SocketTimeoutException());
            retryConnectionTimeScheduler.shutdown();
            stop();
            return;
        }
        try {
            this.session.incrementeConnectionAttempts();
            this.transportLayer.sendPacket(synPacket);
        } catch (IOException e) {
            this.connection.completeExceptionally(new SocketTimeoutException());
            retryConnectionTimeScheduler.shutdown();
            stop();
        }
    }


    //refactor

    public long getConnectionIdRecievingIncoming() {
        return this.session.getConnectionIdReceiving();
    }

    public SessionState getState() {
        return this.session.getState();
    }

    public int getAckNumber() {
        return this.session.getAckNumber();
    }

    public int getSequenceNumber() {
        return this.session.getSequenceNumber();
    }

    public void setAckNumber(int ackNumber) {
        this.session.setAckNumer(ackNumber);
    }

    //for testing must be removed.
    public void setState(SessionState sessionState) {
        this.session.changeState(sessionState);
    }
}
