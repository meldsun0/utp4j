
package net.utp4j.channels.impl.operations;

import net.utp4j.channels.impl.UTPClient;
import net.utp4j.channels.impl.UTPServer;
import net.utp4j.channels.impl.UtpTimestampedPacketDTO;
import net.utp4j.channels.impl.alg.UtpAlgorithm;
import net.utp4j.data.MicroSecondsTimeStamp;
import net.utp4j.data.UtpPacket;
import net.utp4j.data.bytes.UnsignedTypesUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handles the writing job of a channel...
 *
 * @author Ivan Iljkic (i.iljkic@gmail.com)
 */
public class UtpWritingRunnable extends Thread implements Runnable {

    private final ByteBuffer buffer;
    private volatile boolean graceFullInterrupt;
    private final UTPClient channel;
    private boolean isRunning = false;
    private final UtpAlgorithm algorithm;
    private IOException possibleException = null;
    private final MicroSecondsTimeStamp timeStamper;

    private CompletableFuture<Void> writerFuture;
    private static final Logger LOG = LogManager.getLogger(UtpWritingRunnable.class);

    public UtpWritingRunnable(UTPClient channel, ByteBuffer buffer,
                              MicroSecondsTimeStamp timeStamper, CompletableFuture<Void> writerFuture) {
        this.buffer = buffer;
        this.channel = channel;
        this.timeStamper = timeStamper;
        this.writerFuture = writerFuture;
        algorithm = new UtpAlgorithm(timeStamper, channel.getRemoteAdress());
    }

    private void initializeAlgorithm() {
        algorithm.initiateAckPosition(channel.getCurrentSequenceNumber());
        algorithm.setTimeStamper(timeStamper);
        algorithm.setByteBuffer(buffer);
    }

    @Override
    public void run() {
        boolean successfull = true;
        try {
            initializeAlgorithm();
            buffer.flip();
            isRunning = true;
            while (continueSending()) {
                if (!processAcknowledgements()) {
                    LOG.debug("Graceful interrupt due to lack of acknowledgements.");
                    break;
                }
                resendPendingPackets();

                if (algorithm.isTimedOut()) {
                    LOG.debug("Timed out. Stopping transmission.");
                    break;
                }
                sendNextPackets();
            }
        } catch (IOException exp) {
            successfull = false;
            LOG.debug("Something went wrong!");
        } finally {
            finalizeTransmission(successfull);
        }
    }


    private void finalizeTransmission(boolean successful) {
        isRunning = false;
        algorithm.end(buffer.position(), successful);
        LOG.debug("Transmission complete.");
        channel.removeWriter();
        if (successful) {
            writerFuture.complete(null);
        } else {
            writerFuture.completeExceptionally(new RuntimeException("Something went wrong!"));
        }
    }

    private void sendNextPackets() throws IOException {
        while (algorithm.canSendNextPacket() && buffer.hasRemaining()) {
            DatagramPacket nextPacket = getNextPacket();
            channel.sendPacket(nextPacket);
        }
    }

    private void resendPendingPackets() throws IOException {
        Queue<DatagramPacket> packetsToResend = algorithm.getPacketsToResend();
        for (DatagramPacket packet : packetsToResend) {
            packet.setSocketAddress(channel.getRemoteAdress());
            channel.sendPacket(packet);
        }
    }

    private boolean processAcknowledgements() {
        BlockingQueue<UtpTimestampedPacketDTO> packetQueue = channel.getDataGramQueue();
        long waitingTimeMicros = algorithm.getWaitingTimeMicroSeconds();
        try {
            UtpTimestampedPacketDTO packet = packetQueue.poll(waitingTimeMicros, TimeUnit.MICROSECONDS);
            while (packet != null) {
                algorithm.ackRecieved(packet);
                algorithm.removeAcked();
                packet = packetQueue.poll();
            }
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }


    private DatagramPacket getNextPacket() throws IOException {
        int packetSize = algorithm.sizeOfNextPacket();
        int remainingBytes = buffer.remaining();

        if (remainingBytes < packetSize) {
            packetSize = remainingBytes;
        }


        byte[] payload = new byte[packetSize];
        buffer.get(payload);
        UtpPacket utpPacket = channel.getNextDataPacket();
        utpPacket.setPayload(payload);

        int leftInBuffer = buffer.remaining();
        if (leftInBuffer > UnsignedTypesUtil.MAX_UINT) {
            leftInBuffer = (int) (UnsignedTypesUtil.MAX_UINT & 0xFFFFFFFF);
        }
        utpPacket.setWindowSize(leftInBuffer);
//		log.debug("Sending Pkt: " + utpPacket.toString());
        byte[] utpPacketBytes = utpPacket.toByteArray();
        DatagramPacket udpPacket = new DatagramPacket(utpPacketBytes, utpPacketBytes.length, channel.getRemoteAdress());
        algorithm.markPacketOnfly(utpPacket, udpPacket);
        return udpPacket;
    }


    public boolean hasExceptionOccured() {
        return possibleException != null;
    }

    public IOException getException() {
        return possibleException;
    }

    private boolean continueSending() {
        return !graceFullInterrupt && !allPacketsAckedSendAndAcked();
    }

    private boolean allPacketsAckedSendAndAcked() {

//		return finSend && algorithm.areAllPacketsAcked() && !buffer.hasRemaining();
        return algorithm.areAllPacketsAcked() && !buffer.hasRemaining();
    }


    public void graceFullInterrupt() {
        graceFullInterrupt = true;
    }

    public int getBytesSend() {
        return buffer.position();
    }

    public boolean isRunning() {
        return isRunning;
    }
}

/*
*
* on run
* //			if (!buffer.hasRemaining() && !finSend) {
//				UtpPacket fin = channel.getFinPacket();
//				log.debug("Sending FIN");
//				try {
//					channel.finalizeConnection(fin);
//					algorithm.markFinOnfly(fin);
//				} catch (IOException exp) {
//
//				}
//				finSend = true;
//			}
* */