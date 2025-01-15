package utp;

import utp.algo.UtpAlgConfiguration;
import utp.data.MicroSecondsTimeStamp;
import utp.data.SelectiveAckHeaderExtension;
import utp.data.UtpPacket;
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
    private DatagramSocket underlyingUDPSocket;

    private final Object sendLock = new Object();
    private MicroSecondsTimeStamp timeStamper = new MicroSecondsTimeStamp();
    private ScheduledExecutorService retryConnectionTimeScheduler;

    private Optional<UTPServer> server = Optional.empty();
    private Optional<UTPWritingFuture> writer = Optional.empty();
    private Optional<UTPReadingFuture> reader = Optional.empty();

    private AtomicBoolean listen = new AtomicBoolean(false);

    private CompletableFuture<Void> incomingPacketFuture;
    private CompletableFuture<Void> connection;


    public UTPClient() {
        this.session = new Session();
    }

    public UTPClient(DatagramSocket socket, UTPServer server) {
        this.session = new Session();
        this.underlyingUDPSocket = socket;
        this.server = Optional.of(server);
    }


    public CompletableFuture<Void> connect(SocketAddress address, int connectionId) {
        checkNotNull(address, "Address");
        checkArgument(Utils.isConnectionValid(connectionId), "ConnectionId invalid number");
        LOG.info("Starting UTP server listening on {}", connectionId);
        if (!listen.compareAndSet(false, true)) {
            CompletableFuture.failedFuture(new IllegalStateException("Attempted to start an already started server listening on " + connectionId));
        }
        try {
            connection = new CompletableFuture<>();
            this.setUnderlyingUDPSocket(new DatagramSocket());
            this.startListeningIncomingPackets();
            this.session.initConnection(address, connectionId);
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


    private void startListeningIncomingPackets() {
        this.incomingPacketFuture = CompletableFuture.runAsync(() -> {
            while (listen.get()) {
                byte[] buffer = new byte[MAX_UDP_HEADER_LENGTH + MAX_UTP_PACKET_LENGTH];
                DatagramPacket dgpkt = new DatagramPacket(buffer, buffer.length);
                try {
                    this.underlyingUDPSocket.receive(dgpkt);
                    recievePacket(dgpkt);
                } catch (IOException e) {
                    break;
                }
            }
        });
    }


    public void recievePacket(DatagramPacket udpPacket) {
        MessageType messageType = UTPWireMessageDecoder.decode(udpPacket);
        UtpPacket utpPacket = UtpPacket.decode(udpPacket);

        if (messageType == MessageType.ST_STATE && this.session.getState() == SessionState.SYN_SENT) {
            handleConfirmationOfConnection(utpPacket, udpPacket.getSocketAddress());
            return;
        }
        switch (messageType) {
            case ST_RESET -> this.stop();
            case ST_SYN -> handleIncommingConnectionRequest(utpPacket, udpPacket.getSocketAddress());
            case ST_DATA, ST_STATE -> queuePacket(utpPacket, udpPacket);
            case ST_FIN -> handleFinPacket(utpPacket);
            default -> sendResetPacket(udpPacket.getSocketAddress());
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

    private void handleConfirmationOfConnection(UtpPacket utpPacket, SocketAddress socketAddress) {
        if ((utpPacket.getConnectionId() & connectionIdMASK) == this.session.getConnectionIdReceiving()) {
            this.session.connectionConfirmed(utpPacket.getSequenceNumber());
            disableConnectionTimeOutCounter();
            connection.complete(null);
            this.session.printState();
        } else {
            sendResetPacket(socketAddress);
        }
    }

    private void handleIncommingConnectionRequest(UtpPacket utpPacket, SocketAddress socketAddress) {
        //This packet is from a client, but here I am a server
        if (this.session.getState() == CLOSED ||
                (this.session.getState() == CONNECTED && isSameAddressAndId(utpPacket.getConnectionId(), socketAddress))) {
            try {
                this.session.initServerConnection(socketAddress, utpPacket.getConnectionId(), utpPacket.getSequenceNumber());
                session.printState();
                int timestampDifference = timeStamper.utpDifference(timeStamper.utpTimeStamp(), utpPacket.getTimestamp());
                //TODO validate that the seq number is sent!
                UtpPacket packet = ACKMessage.build(timestampDifference,
                        UtpAlgConfiguration.MAX_PACKET_SIZE * 1000L,
                        timeStamper.utpTimeStamp(), this.session.getConnectionIdSending(),
                        this.session.getAckNumber(), NO_EXTENSION);
                sendPacket(packet);
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

    private boolean isSameAddressAndId(long connectionId, SocketAddress addr) {
        return (connectionId & 0xFFFFFFFF) == getConnectionIdRecievingIncoming() && addr.equals(getRemoteAdress());
    }

    private void disableConnectionTimeOutCounter() {
        if (retryConnectionTimeScheduler != null) {
            retryConnectionTimeScheduler.shutdown();
            retryConnectionTimeScheduler = null;
        }
        this.session.resetConnectionAttempts();
    }

    private void queuePacket(UtpPacket utpPacket, DatagramPacket udpPacket) {
        queue.offer(new UtpTimestampedPacketDTO(udpPacket, utpPacket, timeStamper.timeStamp(), timeStamper.utpTimeStamp()));
    }

    private void sendResetPacket(SocketAddress addr) {
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
        this.incomingPacketFuture.complete(null);
        this.session.close();
        this.server.ifPresent(server -> {
            server.unregister(this);
        });
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

    public void sendPacket(DatagramPacket pkt) throws IOException {
        synchronized (sendLock) {
            this.underlyingUDPSocket.send(pkt);
        }
    }

    public void sendPacket(UtpPacket packet) throws IOException {
        sendPacket(UtpPacket.createDatagramPacket(packet, this.session.getTransportAddress()));
    }


    /*********/
    public void setUnderlyingUDPSocket(DatagramSocket underlyingUDPSocket) {
        if (this.underlyingUDPSocket != null) {
            this.underlyingUDPSocket.close();
        }
        this.underlyingUDPSocket = underlyingUDPSocket;
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
            sendPacket(UtpPacket.createDatagramPacket(synPacket, this.session.getTransportAddress()));
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

    //for testing must be removed.
    public void setState(SessionState sessionState) {
        this.session.changeState(sessionState);
    }
}
