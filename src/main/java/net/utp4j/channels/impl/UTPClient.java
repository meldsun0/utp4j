package net.utp4j.channels.impl;

import net.utp4j.channels.UtpSocketState;
import net.utp4j.channels.impl.alg.UtpAlgConfiguration;
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

                UtpPacket synPacket = UtpPacket.builder()
                        .typeVersion(SYN)
                        .sequenceNumber(longToUbyte(1))
                        .payload(new byte[]{1, 2, 3, 4, 5, 6})
                        .connectionId(longToUshort(getConnectionIdRecievingIncoming()))
                        .timestamp(timeStamper.utpTimeStamp())
                        .build();

                sendPacket(UtpPacket.createDatagramPacket(synPacket, this.transportAddress));
                this.state = SYN_SENT;
                this.currentSequenceNumber = Utils.incrementSeqNumber(this.currentSequenceNumber);

                startConnectionTimeOutCounter(synPacket);
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
        if(messageType == MessageType.ST_STATE  && this.state == UtpSocketState.SYN_SENT){
            handleSynAckPacket(udpPacket);
        }
        switch (messageType) {
            case ST_RESET -> this.close();
            case ST_SYN -> handleIncommingConnectionRequest(udpPacket);
            case ST_DATA, ST_STATE ->  handlePacket(udpPacket);
            case ST_FIN ->  handleFinPacket(udpPacket);
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

    private boolean isSameAddressAndId(long id, SocketAddress addr) {
        return (id & 0xFFFFFFFF) == getConnectionIdRecievingIncoming()
                && addr.equals(getRemoteAdress());
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
        UtpPacket ackPacket = createAckPacket(utpPacket, timestampDifference, windowSize);
        sendPacket(UtpPacket.createDatagramPacket(ackPacket, this.transportAddress));
    }


    private void handleIncommingConnectionRequest(DatagramPacket udpPacket) {
        UtpPacket utpPacket = UtpPacket.decode(udpPacket);

        if (acceptSyn(udpPacket)) {
            int timeStamp = timeStamper.utpTimeStamp();
            this.transportAddress = udpPacket.getSocketAddress();
            setConnectionIdsFromPacket(utpPacket);
            this.currentSequenceNumber = Utils.randomSeqNumber();
            setAckNrFromPacketSqNr(utpPacket);
            printState("[Syn recieved] ");
            int timestampDifference = timeStamper.utpDifference(timeStamp,
                    utpPacket.getTimestamp());
            UtpPacket ackPacket = createAckPacket(utpPacket,
                    timestampDifference,
                    UtpAlgConfiguration.MAX_PACKET_SIZE * 1000L);
            try {
                LOG.debug("sending syn ack");
                sendPacket(UtpPacket.createDatagramPacket(ackPacket, this.transportAddress));
                setState(CONNECTED);
            } catch (IOException exp) {
                // TODO: In future?
                this.transportAddress = null;
                this.UTPConnectionIdSending = (short) 0;
                this.UTPConnectionIdReceiving = (short) 0;
                setCurrentAckNumber(0);
                setState(SYN_ACKING_FAILED);
                exp.printStackTrace();
            }
        } else {
            sendResetPacket(udpPacket.getSocketAddress());
        }
    }

    private void sendResetPacket(SocketAddress addr) {
        LOG.debug("Sending RST packet");

    }

    private boolean acceptSyn(DatagramPacket udpPacket) {
        UtpPacket pkt = UtpPacket.decode(udpPacket);
        return getState() == CLOSED
                || (getState() == CONNECTED && isSameAddressAndId(
                pkt.getConnectionId(), udpPacket.getSocketAddress()));
    }

    protected void setAckNrFromPacketSqNr(UtpPacket utpPacket) {
        short ackNumberS = utpPacket.getSequenceNumber();
        setCurrentAckNumber(ackNumberS & 0xFFFF);
    }

    private void setConnectionIdsFromPacket(UtpPacket utpPacket) {
        short connId = utpPacket.getConnectionId();
        int connIdSender = (connId & 0xFFFF);
        int connIdRec = (connId & 0xFFFF) + 1;
        this.UTPConnectionIdSending = connIdSender;
        this.UTPConnectionIdReceiving = connIdRec;


    }

    protected void abortImpl() {
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
        abortImpl();
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
        UtpPacket ackPacket = new UtpPacket();
        ackPacket.setAckNumber(longToUshort(getCurrentAckNumber()));
        SelectiveAckHeaderExtension[] extensions = {extension};
        ackPacket.setExtensions(extensions);
        ackPacket.setTimestampDifference(timestampDifference);
        ackPacket.setTimestamp(timeStamper.utpTimeStamp());
        ackPacket.setConnectionId(longToUshort(this.UTPConnectionIdSending));
        ackPacket.setTypeVersion(STATE);
        ackPacket.setWindowSize(longToUint(windowSize));
        sendPacket(UtpPacket.createDatagramPacket(ackPacket, this.transportAddress));

    }

    public void sendPacket(DatagramPacket pkt) throws IOException {
        synchronized (sendLock) {
            this.underlyingUDPSocket.send(pkt);
        }

    }

    /***/
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
        UtpPacket packet = UtpPacket.createSelectivePacket(
                this.currentAckNumber,
                this.UTPConnectionIdSending,
                timeStamper.utpTimeStamp(),
                STATE, timestampDifference, advertisedWindow,
                headerExtension);
        sendPacket(UtpPacket.createDatagramPacket(packet, this.transportAddress));
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
        abortImpl();
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

    protected UtpPacket createAckPacket(UtpPacket pkt, int timedifference,
                                        long advertisedWindow) {
        UtpPacket ackPacket = new UtpPacket();


        if (pkt.getTypeVersion() != FIN) {
            setAckNrFromPacketSqNr(pkt);
        }
        ackPacket.setAckNumber(longToUshort(getCurrentAckNumber()));

        ackPacket.setTimestampDifference(timedifference);
        ackPacket.setTimestamp(timeStamper.utpTimeStamp());
        ackPacket.setConnectionId(longToUshort(this.UTPConnectionIdSending));
        ackPacket.setTypeVersion(STATE);
        ackPacket.setWindowSize(longToUint(advertisedWindow));
        return ackPacket;
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
