/* Copyright 2013 Ivan Iljkic
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.utp4j.channels.impl;

import net.utp4j.channels.UtpSocketState;
import net.utp4j.channels.impl.alg.UtpAlgConfiguration;
import net.utp4j.channels.impl.conn.ConnectionTimeOutRunnable;
import net.utp4j.channels.impl.conn.UtpConnectFutureImpl;
import net.utp4j.channels.impl.read.UtpReadFutureImpl;
import net.utp4j.channels.impl.read.UtpReadingRunnable;
import net.utp4j.channels.impl.recieve.UtpPacketRecievable;
import net.utp4j.channels.impl.write.UtpWriteFutureImpl;
import net.utp4j.channels.impl.write.UtpWritingRunnable;
import net.utp4j.data.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static net.utp4j.channels.UtpSocketState.*;
import static net.utp4j.data.UtpPacketUtils.*;
import static net.utp4j.data.bytes.UnsignedTypesUtil.*;


public class UTPClient implements
        UtpPacketRecievable {


    /* ID for outgoing packets */
    private long connectionIdSending;

    /* timestamping utility */
    protected MicroSecondsTimeStamp timeStamper = new MicroSecondsTimeStamp();

    /* current sequenceNumber */
    private int sequenceNumber;

    /* ID for incomming packets */
    private long connectionIdRecieving;

    /*
     * address of the remote sockets which this socket is connected to
     */
    protected SocketAddress remoteAddress;

    /* current ack Number */
    protected int ackNumber;

    /* reference to the underlying UDP Socket */
    protected DatagramSocket dgSocket;



    /* lock for the socket state* */
    protected final ReentrantLock stateLock = new ReentrantLock();



    /* Current state of the socket */
    protected volatile UtpSocketState state = null;

    /* Sequencing begin */
    protected static int DEF_SEQ_START = 1;






    private final BlockingQueue<UtpTimestampedPacketDTO> queue = new LinkedBlockingQueue<UtpTimestampedPacketDTO>();


    private UtpWritingRunnable writer;
    private UtpReadingRunnable reader;
    private final Object sendLock = new Object();

    private UTPServer server;
    private ScheduledExecutorService retryConnectionTimeScheduler;
    private int connectionAttempts = 0;

    private int eofPacket;


    private static final Logger LOG = LogManager.getLogger(UTPClient.class);
    private AtomicBoolean listen = new AtomicBoolean(false);
    private final CompletableFuture<Void> initAcceptanceFuture;

    public UTPClient(){
        this.initAcceptanceFuture = new CompletableFuture<>();
        this.state = CLOSED;
    }

    public UTPClient(DatagramSocket socket){
        this.initAcceptanceFuture = new CompletableFuture<>();
        this.state = CLOSED;
        this.dgSocket = socket;
    }

    public UTPClient(DatagramSocket socket, UTPServer server){
        this.initAcceptanceFuture = new CompletableFuture<>();
        this.state = CLOSED;
        this.dgSocket = socket;
        this.server = server;
    }
    public void connect(SocketAddress address, long connectionId) {
        LOG.info("Starting UTP Client connecting to {}", connectionId);
        if (!listen.compareAndSet(false, true)) {

        }
        stateLock.lock();

            try {
                this.dgSocket =  new DatagramSocket();
                        CompletableFuture.runAsync(() -> {
                        while (listen.get()) {
                            byte[] buffer = new byte[MAX_UDP_HEADER_LENGTH + MAX_UTP_PACKET_LENGTH];
                            DatagramPacket dgpkt = new DatagramPacket(buffer, buffer.length);
                            try {
                                dgSocket.receive(dgpkt);
                                recievePacket(dgpkt);
                            } catch (IOException e) {
                                break;
                            }
                        }
                    });

                this.remoteAddress = address;
                this.connectionIdRecieving = connectionId;
                this.connectionIdSending = connectionId+ 1 ;
                this.sequenceNumber = DEF_SEQ_START;

                UtpPacket synPacket = UtpPacketUtils.createSynPacket(longToUshort(getConnectionIdRecieving()), timeStamper.utpTimeStamp());

                sendPacket(synPacket);
                setState(SYN_SENT);
                printState("[Syn send] ");
                incrementSequenceNumber();
                startConnectionTimeOutCounter(synPacket);
            } catch (Exception exp) {
                // DO NOTHING, let's try later with reconnect runnable
            }finally {
            stateLock.unlock();
        }
    }


    public long getConnectionIdsending() {
        return connectionIdSending;
    }

    public boolean isOpen() {
        return state != CLOSED;
    }

    public boolean isConnected() {
        return getState() == UtpSocketState.CONNECTED;
    }

    public long getConnectionIdRecieving() {
        return connectionIdRecieving;
    }

    public UtpSocketState getState() {
        return state;
    }

    public SocketAddress getRemoteAdress() {
        return remoteAddress;
    }

    public int getAckNumber() {
        return ackNumber;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public DatagramSocket getDgSocket() {
        return dgSocket;
    }


    /* debug method to print id's, seq# and ack# */
    protected void printState(String msg) {
        String state = "[ConnID Sending: " + connectionIdSending + "] "
                + "[ConnID Recv: " + connectionIdRecieving + "] [SeqNr. "
                + sequenceNumber + "] [AckNr: " + ackNumber + "]";
        LOG.debug(msg + state);

    }

    /*
     * increments current sequence number unlike TCP, uTp is sequencing its
     * packets, not bytes but the Seq# is only 16 bits, so overflows are likely
     * to happen Seq# == 0 not possible.
     */
    protected void incrementSequenceNumber() {
        int seqNumber = getSequenceNumber() + 1;
        if (seqNumber > MAX_USHORT) {
            seqNumber = 1;
        }
        this.sequenceNumber = seqNumber;
    }

    public void setState(UtpSocketState state) {
        this.state = state;
    }





    /*
     * Handles packet.
     */
    @Override
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
            setState(UtpSocketState.GOT_FIN);
            UtpPacket finPacket = extractUtpPacket(udpPacket);
            long freeBuffer = 0;
            if (reader != null && reader.isRunning()) {
                freeBuffer = reader.getLeftSpaceInBuffer();
            } else {
                freeBuffer = UtpAlgConfiguration.MAX_PACKET_SIZE;
            }
            this.eofPacket = finPacket.getSequenceNumber() & 0xFFFF;
            ackPacket(finPacket, timeStamper.utpDifference(finPacket.getTimestamp()), freeBuffer);
        } catch (IOException exp) {
            exp.printStackTrace();
            // TODO: what to do if exception?
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
        if ((pkt.getConnectionId() & 0xFFFF) == getConnectionIdRecieving()) {
            stateLock.lock();
            setAckNrFromPacketSqNr(pkt);
            setState(CONNECTED);
            printState("[SynAck recieved] ");
            disableConnectionTimeOutCounter();
            this.initAcceptanceFuture.complete(null);
            stateLock.unlock();
        } else {
            sendResetPacket(udpPacket.getSocketAddress());
        }
    }

    private boolean isSameAddressAndId(long id, SocketAddress addr) {
        return (id & 0xFFFFFFFF) == getConnectionIdRecieving()
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

    /**
     * Sends an ack.
     *
     * @param utpPacket           the packet that should be acked
     * @param timestampDifference timestamp difference for tha ack packet
     * @param windowSize          the remaining buffer size.
     * @throws IOException
     */
    public void ackPacket(UtpPacket utpPacket, int timestampDifference,
                          long windowSize) throws IOException {
        UtpPacket ackPacket = createAckPacket(utpPacket, timestampDifference,
                windowSize);
        sendPacket(ackPacket);
    }

    /**
     * setting up a random sequence number.
     */
    public void setupRandomSeqNumber() {
        Random rnd = new Random();
        int max = (int) (MAX_USHORT - 1);
        int rndInt = rnd.nextInt(max);
        this.sequenceNumber = rndInt ;

    }

    private void handleIncommingConnectionRequest(DatagramPacket udpPacket) {
        UtpPacket utpPacket = extractUtpPacket(udpPacket);

        if (acceptSyn(udpPacket)) {
            int timeStamp = timeStamper.utpTimeStamp();
            this.remoteAddress = udpPacket.getSocketAddress();
            setConnectionIdsFromPacket(utpPacket);
            setupRandomSeqNumber();
            setAckNrFromPacketSqNr(utpPacket);
            printState("[Syn recieved] ");
            int timestampDifference = timeStamper.utpDifference(timeStamp,
                    utpPacket.getTimestamp());
            UtpPacket ackPacket = createAckPacket(utpPacket,
                    timestampDifference,
                    UtpAlgConfiguration.MAX_PACKET_SIZE * 1000L);
            try {
                LOG.debug("sending syn ack");
                sendPacket(ackPacket);
                setState(CONNECTED);
            } catch (IOException exp) {
                // TODO: In future?
                this.remoteAddress = null;
                this.connectionIdSending = (short) 0;
                this.connectionIdRecieving = (short) 0;
                setAckNumber(0);
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
        setAckNumber(ackNumberS & 0xFFFF);
    }

    private void setConnectionIdsFromPacket(UtpPacket utpPacket) {
        short connId = utpPacket.getConnectionId();
        int connIdSender = (connId & 0xFFFF);
        int connIdRec = (connId & 0xFFFF) + 1;
        this.connectionIdSending = connIdSender;
        this.connectionIdRecieving = connIdRec;


    }



    protected void unregisterServer() {
         if (server != null) {
            server.unregister(this);
        }
    }
    public void close() {
        if (listen.compareAndSet(true, false)) {
            LOG.info("Stopping UTP server listening on {}", this.connectionIdRecieving);
            unregisterServer();
            if (isReading()) {
                reader.graceFullInterrupt();
            }
            if (isWriting()) {
                writer.graceFullInterrupt();
            }
        } else {
            LOG.warn("An attempt to stop already stopping/stopped UTP server");
        }


    }

    public void write(ByteBuffer src) {
        this.initAcceptanceFuture.whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Task failed with exception: " + ex.getMessage());
            } else {
                UtpWriteFutureImpl future = null;
                try {
                    future = new UtpWriteFutureImpl();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                writer = new UtpWritingRunnable(this, src, timeStamper, future);
                writer.start();
            }
        });
    }

    public BlockingQueue<UtpTimestampedPacketDTO> getDataGramQueue() {
        return queue;
    }

    /**
     * Returns a Data packet with specified header fields already set.
     *
     * @return
     */
    public UtpPacket getNextDataPacket() {
        return createDataPacket();
    }

    /**
     * Returns predefined fin packet
     *
     * @return fin packet.
     */
    public UtpPacket getFinPacket() {
        UtpPacket fin = createDataPacket();
        fin.setTypeVersion(FIN);
        return fin;
    }

    public UtpReadFutureImpl read(ByteBuffer dst) {
        UtpReadFutureImpl readFuture = null;
        try {
            readFuture = new UtpReadFutureImpl();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        reader = new UtpReadingRunnable(this, dst, timeStamper, readFuture);
        reader.start();
        return readFuture;
    }

    /**
     * Creates an Selective Ack packet
     *
     * @param headerExtension     the header extension where the SACK data is stored
     * @param timestampDifference timestamp difference for the ack pcket.
     * @param advertisedWindow    remaining buffer size.
     * @throws IOException
     */
    public void selectiveAckPacket(SelectiveAckHeaderExtension headerExtension,
                                   int timestampDifference, long advertisedWindow) throws IOException {
        UtpPacket sack = createSelectiveAckPacket(headerExtension,
                timestampDifference, advertisedWindow);
        sendPacket(sack);

    }

    private UtpPacket createSelectiveAckPacket(SelectiveAckHeaderExtension headerExtension,
                                               int timestampDifference, long advertisedWindow) {
        UtpPacket ackPacket = new UtpPacket();
        ackPacket.setAckNumber(longToUshort(getAckNumber()));
        ackPacket.setTimestampDifference(timestampDifference);
        ackPacket.setTimestamp(timeStamper.utpTimeStamp());
        ackPacket.setConnectionId(longToUshort(getConnectionIdsending()));
        ackPacket.setWindowSize(longToUint(advertisedWindow));

        ackPacket.setFirstExtension(SELECTIVE_ACK);
        UtpHeaderExtension[] extensions = {headerExtension};
        ackPacket.setExtensions(extensions);
        ackPacket.setTypeVersion(STATE);

        return ackPacket;
    }




    public boolean isReading() {
        return (reader != null && reader.isRunning());
    }

    public boolean isWriting() {
        return (writer != null && writer.isRunning());
    }

    public void ackAlreadyAcked(SelectiveAckHeaderExtension extension, int timestampDifference,
                                long windowSize) throws IOException {
        UtpPacket ackPacket = new UtpPacket();
        ackPacket.setAckNumber(longToUshort(getAckNumber()));
        SelectiveAckHeaderExtension[] extensions = {extension};
        ackPacket.setExtensions(extensions);
        ackPacket.setTimestampDifference(timestampDifference);
        ackPacket.setTimestamp(timeStamper.utpTimeStamp());
        ackPacket.setConnectionId(longToUshort(getConnectionIdsending()));
        ackPacket.setTypeVersion(STATE);
        ackPacket.setWindowSize(longToUint(windowSize));
        sendPacket(ackPacket);

    }

    public void sendPacket(DatagramPacket pkt) throws IOException {
        synchronized (sendLock) {
            getDgSocket().send(pkt);
        }

    }

    /* general method to send a packet, will be wrapped by a UDP Packet */
    public void sendPacket(UtpPacket packet) throws IOException {
        if (packet != null) {
            byte[] utpPacketBytes = packet.toByteArray();
            int length = packet.getPacketLength();
            DatagramPacket pkt = new DatagramPacket(utpPacketBytes, length,
                    getRemoteAdress());
            sendPacket(pkt);
        }
    }

    public void setAckNumber(int ackNumber) {
        this.ackNumber = ackNumber;
    }

    public void setServer(UTPServer UTPServer) {
        this.server = UTPServer;

    }


    public void returnFromReading() {
        reader = null;
        //TODO: dispatch:
        if (!isWriting()) {
            this.state = UtpSocketState.CONNECTED;
        }
    }

    public void removeWriter() {
        writer = null;
    }

    /*
     * Start a connection time out counter which will frequently resend the syn packet.
     */
    protected void startConnectionTimeOutCounter(UtpPacket synPacket) {
        retryConnectionTimeScheduler = Executors
                .newSingleThreadScheduledExecutor();
        ConnectionTimeOutRunnable runnable = new ConnectionTimeOutRunnable(
                synPacket, this, stateLock);
        LOG.debug("starting scheduler");
        // retryConnectionTimeScheduler.schedule(runnable, 2, TimeUnit.SECONDS);
        retryConnectionTimeScheduler.scheduleWithFixedDelay(runnable,
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
        this.sequenceNumber = DEF_SEQ_START;
        this.remoteAddress = null;
        unregisterServer();
        setState(CLOSED);
        retryConnectionTimeScheduler.shutdown();
        retryConnectionTimeScheduler = null;
        this.initAcceptanceFuture.completeExceptionally(new RuntimeException("Something went connecting!"));
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
                        sendPacket(synPacket);
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

    public void setTimetamper(MicroSecondsTimeStamp stamp) {
        this.timeStamper = stamp;

    }

    /*
     * Creates an ACK packet.
     */
    protected UtpPacket createAckPacket(UtpPacket pkt, int timedifference,
                                        long advertisedWindow) {
        UtpPacket ackPacket = new UtpPacket();
        if (pkt.getTypeVersion() != FIN) {
            setAckNrFromPacketSqNr(pkt);
        }
        ackPacket.setAckNumber(longToUshort(getAckNumber()));

        ackPacket.setTimestampDifference(timedifference);
        ackPacket.setTimestamp(timeStamper.utpTimeStamp());
        ackPacket.setConnectionId(longToUshort(getConnectionIdsending()));
        ackPacket.setTypeVersion(STATE);
        ackPacket.setWindowSize(longToUint(advertisedWindow));
        return ackPacket;
    }

    protected UtpPacket createDataPacket() {
        UtpPacket pkt = new UtpPacket();
        pkt.setSequenceNumber(longToUshort(getSequenceNumber()));
        incrementSequenceNumber();
        pkt.setAckNumber(longToUshort(getAckNumber()));
        pkt.setConnectionId(longToUshort(getConnectionIdsending()));
        pkt.setTimestamp(timeStamper.utpTimeStamp());
        pkt.setTypeVersion(UtpPacketUtils.DATA);
        return pkt;
    }

    public void setRemoteAddress(InetSocketAddress localhost) {
        this.remoteAddress = localhost;
    }
}
