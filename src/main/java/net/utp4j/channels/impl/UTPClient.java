package net.utp4j.channels.impl;

import jdk.jshell.execution.Util;
import net.utp4j.channels.UtpSocketState;
import net.utp4j.channels.impl.alg.UtpAlgConfiguration;
import net.utp4j.channels.impl.operations.UTPReadingFuture;
import net.utp4j.channels.impl.operations.UTPWritingFuture;
import net.utp4j.data.*;
import net.utp4j.data.type.Bytes16;
import net.utp4j.data.util.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tuweni.bytes.Bytes;

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
    private long connectionIdSendingOutgoing;
    private long connectionIdRecievingIncoming;
    private int currentAckNumber;
    private int currentSequenceNumber;
    private SocketAddress remoteAddressWhichThisSocketIsConnectedTo;
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

                this.remoteAddressWhichThisSocketIsConnectedTo = address;
                this.connectionIdRecievingIncoming = connectionId;
                this.connectionIdSendingOutgoing = connectionId + 1;

                this.currentSequenceNumber = DEF_SEQ_START;

                UtpPacket synPacket = UtpPacketUtils.createSynPacket(longToUshort(getConnectionIdRecievingIncoming()), timeStamper.utpTimeStamp());

                sendPacket(UtpPacket.createDatagramPacket(synPacket, this.remoteAddressWhichThisSocketIsConnectedTo));
                this.state = SYN_SENT;

                printState("[Syn send] ");
                this.currentSequenceNumber = Utils.incrementSeqNumber(this.currentSequenceNumber);

                startConnectionTimeOutCounter(synPacket);
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
        String state = "[ConnID Sending: " + connectionIdSendingOutgoing + "] "
                + "[ConnID Recv: " + connectionIdRecievingIncoming + "] [SeqNr. "
                + currentSequenceNumber + "] [AckNr: " + currentAckNumber + "]";
        LOG.debug(msg + state);

    }


    public void setState(UtpSocketState state) {
        this.state = state;
    }


    public void recievePacket(DatagramPacket udpPacket) {
        if (isSynAckPacket(udpPacket)) {
            handleSynAckPacket(udpPacket);
        } else if (isResetPacket(udpPacket)) {
            handleResetPacket(udpPacket);
        } else if (isSynPkt(udpPacket)) {
            handleIncommingConnectionRequest(udpPacket);
        } else if (isDataPacket(udpPacket)) {
            handlePacket(udpPacket);
        } else if (isStatePacket(udpPacket)) {
            handlePacket(udpPacket);
        } else if (isFinPacket(udpPacket)) {
            handleFinPacket(udpPacket);
        } else {
            sendResetPacket(udpPacket.getSocketAddress());
        }

    }

    private void handleFinPacket(DatagramPacket udpPacket) {
        stateLock.lock();
        try {
            this.state = UtpSocketState.GOT_FIN;

            UtpPacket finPacket = extractUtpPacket(udpPacket);
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

    private void handleResetPacket(DatagramPacket udpPacket) {
        this.close();
    }

    private boolean isSynAckPacket(DatagramPacket udpPacket) {
        return isStatePacket(udpPacket) && getState() == UtpSocketState.SYN_SENT;
    }

    private void handleSynAckPacket(DatagramPacket udpPacket) {
        UtpPacket pkt = extractUtpPacket(udpPacket);
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
        UtpPacket utpPacket = extractUtpPacket(udpPacket);
        queue.offer(new UtpTimestampedPacketDTO(udpPacket, utpPacket,
                timeStamper.timeStamp(), timeStamper.utpTimeStamp()));

    }


    public void ackPacket(UtpPacket utpPacket, int timestampDifference, long windowSize) throws IOException {
        UtpPacket ackPacket = createAckPacket(utpPacket, timestampDifference, windowSize);
        sendPacket(UtpPacket.createDatagramPacket(ackPacket, this.remoteAddressWhichThisSocketIsConnectedTo));
    }


    private void handleIncommingConnectionRequest(DatagramPacket udpPacket) {
        UtpPacket utpPacket = extractUtpPacket(udpPacket);

        if (acceptSyn(udpPacket)) {
            int timeStamp = timeStamper.utpTimeStamp();
            this.remoteAddressWhichThisSocketIsConnectedTo = udpPacket.getSocketAddress();
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
                sendPacket(UtpPacket.createDatagramPacket(ackPacket, this.remoteAddressWhichThisSocketIsConnectedTo));
                setState(CONNECTED);
            } catch (IOException exp) {
                // TODO: In future?
                this.remoteAddressWhichThisSocketIsConnectedTo = null;
                this.connectionIdSendingOutgoing = (short) 0;
                this.connectionIdRecievingIncoming = (short) 0;
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
        UtpPacket pkt = extractUtpPacket(udpPacket);
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
        this.connectionIdSendingOutgoing = connIdSender;
        this.connectionIdRecievingIncoming = connIdRec;


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
        ackPacket.setConnectionId(longToUshort(this.connectionIdSendingOutgoing));
        ackPacket.setTypeVersion(STATE);
        ackPacket.setWindowSize(longToUint(windowSize));
        sendPacket(UtpPacket.createDatagramPacket(ackPacket, this.remoteAddressWhichThisSocketIsConnectedTo));

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
                        this.connectionIdSendingOutgoing,
                        timeStamper.utpTimeStamp(),
                        UtpPacketUtils.DATA);
        this.currentSequenceNumber = Utils.incrementSeqNumber(this.currentSequenceNumber);
        return utpPacket;
    }


    public UtpPacket getFinPacket() {
        UtpPacket finPacket = UtpPacket
                .createPacket(this.currentSequenceNumber,
                        this.currentAckNumber,
                        this.connectionIdSendingOutgoing,
                        timeStamper.utpTimeStamp(),
                        UtpPacketUtils.FIN);
        this.currentSequenceNumber = Utils.incrementSeqNumber(this.currentSequenceNumber);
        return finPacket;
    }


    public void selectiveAckPacket(SelectiveAckHeaderExtension headerExtension,
                                   int timestampDifference, long advertisedWindow) throws IOException {
        UtpPacket packet = UtpPacket.createSelectivePacket(
                this.currentAckNumber,
                this.connectionIdSendingOutgoing,
                timeStamper.utpTimeStamp(),
                STATE, timestampDifference, advertisedWindow,
                headerExtension);
        sendPacket(UtpPacket.createDatagramPacket(packet, this.remoteAddressWhichThisSocketIsConnectedTo));
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
        this.remoteAddressWhichThisSocketIsConnectedTo = null;
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
                        sendPacket(UtpPacket.createDatagramPacket(synPacket, this.remoteAddressWhichThisSocketIsConnectedTo));
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
        ackPacket.setConnectionId(longToUshort(this.connectionIdSendingOutgoing));
        ackPacket.setTypeVersion(STATE);
        ackPacket.setWindowSize(longToUint(advertisedWindow));
        return ackPacket;
    }


    public long getConnectionIdRecievingIncoming() {
        return connectionIdRecievingIncoming;
    }

    public UtpSocketState getState() {
        return state;
    }

    public SocketAddress getRemoteAdress() {
        return remoteAddressWhichThisSocketIsConnectedTo;
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

    public void setRemoteAddressWhichThisSocketIsConnectedTo(InetSocketAddress localhost) {
        this.remoteAddressWhichThisSocketIsConnectedTo = localhost;
    }

    public void removeWriter() {
        writer = null;
    }
}
