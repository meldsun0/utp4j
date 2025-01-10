package net.utp4j.channels.impl;

import net.utp4j.channels.UtpSocketState;
import net.utp4j.channels.impl.alg.UtpAlgConfiguration;
import net.utp4j.channels.impl.message.ACKMessage;
import net.utp4j.channels.impl.message.InitConnectionMessage;
import net.utp4j.channels.impl.message.MessageType;
import net.utp4j.channels.impl.message.UTPWireMessageDecoder;
import net.utp4j.channels.impl.operations.UTPReadingFuture;
import net.utp4j.channels.impl.operations.UTPWritingFuture;
import net.utp4j.data.*;
import net.utp4j.data.util.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static net.utp4j.channels.UtpSocketState.*;
import static net.utp4j.data.UtpPacketUtils.*;
import static net.utp4j.data.bytes.UnsignedTypesUtil.*;


public class UTPClient {

    //State
    private long UTPConnectionIdSending;
    private long UTPConnectionIdReceiving;
    private SocketAddress transportAddress;


    private int currentAckNumber;
    private int currentSequenceNumber;
    private volatile UtpSocketState state = null;
    private static int DEF_SEQ_START = 1;
    private int connectionAttempts = 0;
    private int eofPacket;

    private MicroSecondsTimeStamp timeStamper = new MicroSecondsTimeStamp();
    private final BlockingQueue<UtpTimestampedPacketDTO> queue = new LinkedBlockingQueue<UtpTimestampedPacketDTO>();


    private DatagramSocket underlyingUDPSocket;

    private final ReentrantLock stateLock = new ReentrantLock();
    private final Object sendLock = new Object();


    private ScheduledExecutorService retryConnectionTimeScheduler;
    private UTPServer server;
    private UTPWritingFuture writer;
    private UTPReadingFuture reader;
    private AtomicBoolean listen = new AtomicBoolean(false);
    private CompletableFuture<Void> incomingConnectionFuture;
    private CompletableFuture<Void> connection;
    private static final Logger LOG = LogManager.getLogger(UTPClient.class);

    public UTPClient() {
        this.state = CLOSED;
    }

    public UTPClient(DatagramSocket socket, UTPServer server) {
        this.state = CLOSED;
        this.underlyingUDPSocket = socket;
        this.server = server;
    }


    public CompletableFuture<Void> connect(SocketAddress address, int connectionId) {
        checkNotNull(address, "Address");
        checkArgument(Utils.isConnectionValid(connectionId), "ConnectionId invalid number");
        LOG.info("Starting UTP server listening on {}", connectionId);
        stateLock.lock();
        if (!listen.compareAndSet(false, true)) {
            CompletableFuture.failedFuture(new IllegalStateException("Attempted to start an already started server listening on " + connectionId));
        }
        try {
            connection = new CompletableFuture<>();
            try {
                this.setUnderlyingUDPSocket(new DatagramSocket());
                startReading();

                this.transportAddress = address;
                this.UTPConnectionIdReceiving = connectionId;
                this.UTPConnectionIdSending = connectionId + 1;
                this.currentSequenceNumber = DEF_SEQ_START;

                UtpPacket message = InitConnectionMessage.build(timeStamper.utpTimeStamp(), this.UTPConnectionIdReceiving);
                sendPacket(message);
                this.state = SYN_SENT;
                this.currentSequenceNumber = Utils.incrementSeqNumber(this.currentSequenceNumber);

                startConnectionTimeOutCounter(message);
                printState("[Syn send] ");
            } catch (IOException exp) {
                //TODO reconnect
            }
        } finally {
            stateLock.unlock();
        }
        return connection;
    }


    private void startReading() {
        this.incomingConnectionFuture = CompletableFuture.runAsync(() -> {
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


    /* debug method to print id's, seq# and ack# */
    protected void printState(String msg) {
        String state = "[ConnID Sending: " + UTPConnectionIdSending + "] "
                + "[ConnID Recv: " + UTPConnectionIdReceiving + "] [SeqNr. "
                + currentSequenceNumber + "] [AckNr: " + currentAckNumber + "]";
        LOG.debug(msg + state);
    }


    public void setState(UtpSocketState state) {
        this.state = state;
    }


    public void recievePacket(DatagramPacket udpPacket) {
        MessageType messageType = UTPWireMessageDecoder.decode(udpPacket);
        UtpPacket utpPacket  = UtpPacket.decode(udpPacket);
        if (messageType == MessageType.ST_STATE && this.state == UtpSocketState.SYN_SENT) {
            handleSynAckPacket(udpPacket);
        }
        switch (messageType) {
            case ST_RESET -> this.close();
            case ST_SYN -> handleIncommingConnectionRequest(utpPacket, udpPacket.getSocketAddress());
            case ST_DATA, ST_STATE -> handlePacket(udpPacket);
            case ST_FIN -> handleFinPacket(udpPacket);
            default -> sendResetPacket(udpPacket.getSocketAddress());
        }
    }

    private void handleFinPacket(DatagramPacket udpPacket) {
        stateLock.lock();
        try {
            this.state = UtpSocketState.GOT_FIN;

            UtpPacket finPacket = UtpPacket.decode(udpPacket);
            long freeBuffer = 0;
            if (reader != null && reader.isAlive()) {
                freeBuffer = reader.getLeftSpaceInBuffer();
            } else {
                freeBuffer = UtpAlgConfiguration.MAX_PACKET_SIZE;
            }
            this.eofPacket = finPacket.getSequenceNumber() & 0xFFFF;
            ackPacket(finPacket, timeStamper.utpDifference(finPacket.getTimestamp()), freeBuffer);
        } catch (IOException exp) {
            exp.printStackTrace();
        } finally {
            stateLock.unlock();
        }
    }

    private void handleSynAckPacket(DatagramPacket udpPacket) {
        UtpPacket pkt = UtpPacket.decode(udpPacket);
        if ((pkt.getConnectionId() & 0xFFFF) == getConnectionIdRecievingIncoming()) {
            stateLock.lock();
            setAckNrFromPacketSqNr(pkt);
            setState(CONNECTED);
            printState("[SynAck recieved] ");
            disableConnectionTimeOutCounter();
            connection.complete(null);
            stateLock.unlock();
        } else {
            sendResetPacket(udpPacket.getSocketAddress());
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
        connectionAttempts = 0;
    }

    public int getConnectionAttempts() {
        return connectionAttempts;
    }

    public void incrementConnectionAttempts() {
        connectionAttempts++;
    }

    private void handlePacket(DatagramPacket udpPacket) {
        UtpPacket utpPacket = UtpPacket.decode(udpPacket);
        queue.offer(new UtpTimestampedPacketDTO(udpPacket, utpPacket, timeStamper.timeStamp(), timeStamper.utpTimeStamp()));

    }


    public void ackPacket(UtpPacket utpPacket, int timestampDifference, long windowSize) throws IOException {
        if (utpPacket.getTypeVersion() != FIN) {
            setAckNrFromPacketSqNr(utpPacket);
        }
        //TODO validate that the seq number is sent!
        UtpPacket packet = ACKMessage.build(timestampDifference,
                windowSize, this.timeStamper.utpTimeStamp(),
                this.UTPConnectionIdSending, this.currentAckNumber);
        sendPacket(packet);
    }


    private void handleIncommingConnectionRequest(UtpPacket utpPacket, SocketAddress socketAddress) {
        //This packet is from a client, but here I am a server
        if (this.state == CLOSED ||
                (this.state == CONNECTED && isSameAddressAndId(utpPacket.getConnectionId(), socketAddress))) {
            try {
                //STATE
                int timeStamp = timeStamper.utpTimeStamp();
                this.transportAddress = socketAddress;
                short connId = utpPacket.getConnectionId();
                int connIdSender = (connId & 0xFFFF);
                int connIdRec = (connId & 0xFFFF) + 1;
                this.UTPConnectionIdSending = connIdSender;
                this.UTPConnectionIdReceiving = connIdRec;
                this.currentSequenceNumber = Utils.randomSeqNumber();
                short ackNumberS = utpPacket.getSequenceNumber();
                this.currentAckNumber = ackNumberS & 0xFFFF;


                printState("[Syn recieved] ");

                int timestampDifference = timeStamper.utpDifference(timeStamp, utpPacket.getTimestamp());
                //TODO validate that the seq number is sent!
                UtpPacket packet = ACKMessage.build(timestampDifference, UtpAlgConfiguration.MAX_PACKET_SIZE * 1000L, timeStamper.utpTimeStamp(), this.UTPConnectionIdSending, this.currentAckNumber);
                sendPacket(packet);
                this.state = CONNECTED;

                LOG.info("sending syn ack");
            } catch (IOException exp) {
                // TODO: In future?
                this.transportAddress = null;
                this.UTPConnectionIdSending = (short) 0;
                this.UTPConnectionIdReceiving = (short) 0;
                this.currentAckNumber = 0;
                this.state = SYN_ACKING_FAILED;
                exp.printStackTrace();
            }
        } else {
            sendResetPacket(socketAddress);
        }
    }

    private void sendResetPacket(SocketAddress addr) {
        LOG.debug("Sending RST packet");

    }

    private boolean acceptSyn(DatagramPacket udpPacket) {
        UtpPacket pkt = UtpPacket.decode(udpPacket);
        return this.state == CLOSED || (this.state == CONNECTED && isSameAddressAndId(
                pkt.getConnectionId(), udpPacket.getSocketAddress()));
    }

    protected void setAckNrFromPacketSqNr(UtpPacket utpPacket) {
        short ackNumberS = utpPacket.getSequenceNumber();
        setCurrentAckNumber(ackNumberS & 0xFFFF);
    }


    private void stop() {
        if (listen.compareAndSet(true, false)) {
            this.incomingConnectionFuture.complete(null);
            if (server != null) {
                server.unregister(this);
            }
        } else {
            LOG.warn("An attempt to stop already stopping/stopped UTP server");
        }
    }

    public CompletableFuture<Void> write(ByteBuffer src) {
        this.writer = new UTPWritingFuture(this, src, timeStamper);
        return writer.startWriting();

    }

    public CompletableFuture<Void> read(ByteBuffer dst) {
        this.reader = new UTPReadingFuture(this, dst, timeStamper);
        return reader.startReading();
    }

    public BlockingQueue<UtpTimestampedPacketDTO> getDataGramQueue() {
        return queue;
    }


    public void close() {
        stop();
        if (isReading()) {
            reader.graceFullInterrupt();
        }

        if (isWriting()) {
            writer.graceFullInterrupt();
        }
    }

    public boolean isReading() {
        return (reader != null && reader.isAlive());
    }

    public boolean isWriting() {
        return (writer != null && writer.isAlive());
    }

    public void ackAlreadyAcked(SelectiveAckHeaderExtension extension, int timestampDifference,
                                long windowSize) throws IOException {

        SelectiveAckHeaderExtension[] extensions = {extension};

        UtpPacket packet = UtpPacket.builder()
                .ackNumber(longToUshort(getCurrentAckNumber()))
                .extensions(extensions)
                .timestampDifference(timestampDifference)
                .timestamp(timeStamper.utpTimeStamp())
                .connectionId(longToUshort(this.UTPConnectionIdSending))
                .typeVersion(STATE)
                .windowSize(longToUint(windowSize))
                .build();
        this.sendPacket(packet);
    }

    public void sendPacket(DatagramPacket pkt) throws IOException {
        synchronized (sendLock) {
            this.underlyingUDPSocket.send(pkt);
        }
    }

    public void sendPacket(UtpPacket packet) throws IOException {
        sendPacket(UtpPacket.createDatagramPacket(packet, this.transportAddress));
    }

    public UtpPacket getNextDataPacket() {
        UtpPacket utpPacket = UtpPacket
                .createPacket(this.currentSequenceNumber,
                        this.currentAckNumber,
                        this.UTPConnectionIdSending,
                        timeStamper.utpTimeStamp(),
                        UtpPacketUtils.DATA);
        this.currentSequenceNumber = Utils.incrementSeqNumber(this.currentSequenceNumber);
        return utpPacket;
    }


    public UtpPacket getFinPacket() {
        UtpPacket finPacket = UtpPacket
                .createPacket(this.currentSequenceNumber,
                        this.currentAckNumber,
                        this.UTPConnectionIdSending,
                        timeStamper.utpTimeStamp(),
                        UtpPacketUtils.FIN);
        this.currentSequenceNumber = Utils.incrementSeqNumber(this.currentSequenceNumber);
        return finPacket;
    }


    public void selectiveAckPacket(SelectiveAckHeaderExtension headerExtension,
                                   int timestampDifference, long advertisedWindow) throws IOException {
        UtpHeaderExtension[] extensions = {headerExtension};
        UtpPacket packet = UtpPacket.builder().ackNumber(longToUshort(this.currentAckNumber))
                .connectionId(longToUshort(this.UTPConnectionIdSending))
                .timestamp(timeStamper.utpTimeStamp())
                .typeVersion(STATE)
                .timestampDifference(timestampDifference)
                .windowSize(longToUint(advertisedWindow))
                .firstExtension(SELECTIVE_ACK)
                .extensions(extensions).build();
        sendPacket(packet);
    }

    /***/
    public void setUnderlyingUDPSocket(DatagramSocket underlyingUDPSocket) {
        if (this.underlyingUDPSocket != null) {
            this.underlyingUDPSocket.close();
        }
        this.underlyingUDPSocket = underlyingUDPSocket;
    }


    public void returnFromReading() {
        reader = null;
        //TODO: dispatch:
        if (!isWriting()) {
            this.state = UtpSocketState.CONNECTED;
        }
    }

    //****/
    /*
     * Start a connection time out counter which will frequently resend the syn packet.
     */
    protected void startConnectionTimeOutCounter(UtpPacket synPacket) {
        LOG.debug("starting scheduler");
        retryConnectionTimeScheduler = Executors.newSingleThreadScheduledExecutor();
        retryConnectionTimeScheduler.scheduleWithFixedDelay(() -> {
                    this.resendSynPacket(synPacket);
                },
                UtpAlgConfiguration.CONNECTION_ATTEMPT_INTERVALL_MILLIS,
                UtpAlgConfiguration.CONNECTION_ATTEMPT_INTERVALL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Called by the time out counter {@see ConnectionTimeOutRunnable}
     *
     * @param exp - is optional.
     */
    public void connectionFailed(IOException exp) {
        LOG.debug("got failing message");
        this.currentSequenceNumber = DEF_SEQ_START;
        this.transportAddress = null;
        stop();
        setState(CLOSED);
        retryConnectionTimeScheduler.shutdown();
        retryConnectionTimeScheduler = null;
        connection.completeExceptionally(new RuntimeException("Something went wrong!"));

    }

    /**
     * Resends syn packet. called by {@see ConnectionTimeOutRunnable}
     *
     * @param synPacket
     */
    public void resendSynPacket(UtpPacket synPacket) {
        stateLock.lock();
        try {
            int attempts = getConnectionAttempts();
            LOG.debug("attempt: " + attempts);
            if (getState() == UtpSocketState.SYN_SENT) {
                try {
                    if (attempts < UtpAlgConfiguration.MAX_CONNECTION_ATTEMPTS) {
                        incrementConnectionAttempts();
                        LOG.debug("REATTEMPTING CONNECTION");
                        sendPacket(UtpPacket.createDatagramPacket(synPacket, this.transportAddress));
                    } else {
                        connectionFailed(new SocketTimeoutException());
                    }
                } catch (IOException e) {
                    if (attempts >= UtpAlgConfiguration.MAX_CONNECTION_ATTEMPTS) {
                        connectionFailed(e);
                    } // else ignore, try in next attempt
                }
            }
        } finally {
            stateLock.unlock();
        }

    }


    public long getConnectionIdRecievingIncoming() {
        return UTPConnectionIdReceiving;
    }

    public UtpSocketState getState() {
        return state;
    }

    public SocketAddress getRemoteAdress() {
        return transportAddress;
    }

    public int getCurrentAckNumber() {
        return currentAckNumber;
    }

    public int getCurrentSequenceNumber() {
        return currentSequenceNumber;
    }


    public void setTimetamper(MicroSecondsTimeStamp stamp) {
        this.timeStamper = stamp;
    }

    public void setCurrentAckNumber(int currentAckNumber) {
        this.currentAckNumber = currentAckNumber;
    }

    public void setTransportAddress(InetSocketAddress localhost) {
        this.transportAddress = localhost;
    }

    public void removeWriter() {
        writer = null;
    }
}
