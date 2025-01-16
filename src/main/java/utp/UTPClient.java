package utp;

import utp.algo.UtpAlgConfiguration;
import utp.data.MicroSecondsTimeStamp;
import utp.data.SelectiveAckHeaderExtension;
import utp.data.UtpPacket;
import utp.network.TransportLayer;
import utp.operations.UTPReadingFuture;
import utp.operations.UTPWritingFuture;

import utp.data.util.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utp.message.*;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;

import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static utp.SessionState.*;
import static utp.data.UtpPacketUtils.*;


public class UTPClient {

    private static final Logger LOG = LogManager.getLogger(UTPClient.class);
    private int connectionIdMASK = 0xFFFF;

    private Session session;

    private final BlockingQueue<UtpTimestampedPacketDTO> queue = new LinkedBlockingQueue<UtpTimestampedPacketDTO>();

    private MicroSecondsTimeStamp timeStamper = new MicroSecondsTimeStamp();
    private ScheduledExecutorService retryConnectionTimeScheduler;

    private Optional<UTPWritingFuture> writer = Optional.empty();
    private Optional<UTPReadingFuture> reader = Optional.empty();

    private AtomicBoolean listen = new AtomicBoolean(false);

    private CompletableFuture<Void> connection;
    private final TransportLayer transportLayer;

    public UTPClient(final TransportLayer transportLayer) {
        this.session = new Session();
        this.transportLayer = transportLayer;
    }

    public CompletableFuture<Void> connect(int connectionId) {
        checkNotNull(this.transportLayer.getRemoteAddress(), "Address");
        checkArgument(Utils.isConnectionValid(connectionId), "ConnectionId invalid number");
        LOG.info("Send Connection message to {}", connectionId);
        if (!listen.compareAndSet(false, true)) {
            CompletableFuture.failedFuture(new IllegalStateException("Attempted to start an already started server listening on " + connectionId));
        }
        try {
            connection = new CompletableFuture<>();
            this.session.initConnection(this.transportLayer.getRemoteAddress(), connectionId);
            UtpPacket message = InitConnectionMessage.build(timeStamper.utpTimeStamp(), connectionId);
            sendPacket(message);
            this.session.updateStateOnConnectionInitSuccess();

            startConnectionTimeOutCounter(message);
            this.session.printState();
        } catch (IOException exp) {
            //TODO reconnect
        }
        return connection;
    }

    public void receivePacket(UtpPacket utpPacket) {

        switch (utpPacket.getMessageType()) {
            case ST_RESET, ST_SYN -> this.stop();
            case ST_DATA, ST_STATE -> queuePacket(utpPacket);
            case ST_FIN -> handleFinPacket(utpPacket);
            default -> sendResetPacket();
        }
    }

    private void handleFinPacket(UtpPacket packet) {
        try {
            this.session.changeState(GOT_FIN);
            long freeBuffer = (reader.isPresent() && reader.get().isAlive()) ? reader.get().getLeftSpaceInBuffer() : UtpAlgConfiguration.MAX_PACKET_SIZE;
            UtpPacket ackPacket = buildACKPacket(packet, timeStamper.utpDifference(packet.getTimestamp()), freeBuffer);
            sendPacket(ackPacket);
        } catch (IOException e) {
            //TODO error when sending ack
        }

    }

    private void handleConfirmationOfConnection(UtpPacket utpPacket) {
        if ((utpPacket.getConnectionId() & connectionIdMASK) == this.session.getConnectionIdReceiving()) {
            this.session.connectionConfirmed(utpPacket.getSequenceNumber());
            disableConnectionTimeOutCounter();
            connection.complete(null);
            this.session.printState();
        } else {
            sendResetPacket();
        }
    }


    private void disableConnectionTimeOutCounter() {
        if (retryConnectionTimeScheduler != null) {
            retryConnectionTimeScheduler.shutdown();
            retryConnectionTimeScheduler = null;
        }
        this.session.resetConnectionAttempts();
    }

    private void queuePacket(UtpPacket utpPacket) {
        if (utpPacket.getMessageType() == MessageType.ST_STATE && this.session.getState() == SessionState.SYN_SENT) {
            handleConfirmationOfConnection(utpPacket);
            return;
        }

        queue.offer(new UtpTimestampedPacketDTO(utpPacket, timeStamper.timeStamp(), timeStamper.utpTimeStamp()));
    }

    private void sendResetPacket() {
        //TODO
        LOG.debug("Sending RST packet MUST BE IMPLEMENTED");

    }

    public CompletableFuture<Void> write(ByteBuffer buffer) {
        //TODO handle case when connection is not done yet.
        this.writer = Optional.of(new UTPWritingFuture(this, buffer, timeStamper));
        return writer.get().startWriting();
    }

    public CompletableFuture<Void> read(ByteBuffer dst) {
        //TODO handle case when connection is not done yet.
        this.reader = Optional.of(new UTPReadingFuture(this, dst, timeStamper));
        return reader.get().startReading();
    }

    public BlockingQueue<UtpTimestampedPacketDTO> getQueue() {
        return queue;
    }

    public void stop() {
        if (!listen.compareAndSet(true, false)) {
            LOG.warn("An attempt to stop an already stopping/stopped UTP server");
            return;
        }
        this.session.close();
        this.reader.ifPresent(UTPReadingFuture::graceFullInterrupt);
        this.writer.ifPresent(UTPWritingFuture::graceFullInterrupt);
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

    public void sendPacket(UtpPacket packet) throws IOException {
       this.transportLayer.sendPacket(packet);
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

    public SocketAddress getRemoteAdress() {
        return this.session.getTransportAddress();
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

    public void setTransportAddress(InetSocketAddress localhost) {
        this.session.setTransportAddress(localhost);
    }

    public boolean isAlive() {
        return this.listen.get();
    }


    //for testing must be removed.
    public void setState(SessionState sessionState) {
        this.session.changeState(sessionState);
    }
}
