
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

public class UtpWritingRunnable extends Thread implements Runnable {

    private static final Logger LOG = LogManager.getLogger(UtpWritingRunnable.class);

    private final ByteBuffer buffer;
    private volatile boolean graceFullInterrupt;
    private final UTPClient channel;
    private final UtpAlgorithm algorithm;
    private final MicroSecondsTimeStamp timeStamper;

    private CompletableFuture<Void> writerFuture;

    public UtpWritingRunnable(UTPClient channel, ByteBuffer buffer,
                              MicroSecondsTimeStamp timeStamper, CompletableFuture<Void> writerFuture) {
        this.buffer = buffer;
        this.channel = channel;
        this.timeStamper = timeStamper;
        this.writerFuture = writerFuture;
        algorithm = new UtpAlgorithm(timeStamper, channel.getRemoteAdress());
    }


    @Override
    public void run() {
        boolean successfull = true;
        try {
            initializeAlgorithm();
            buffer.flip();
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

    private void initializeAlgorithm() {
        algorithm.initiateAckPosition(channel.getCurrentSequenceNumber());
        algorithm.setTimeStamper(timeStamper);
        algorithm.setByteBuffer(buffer);
    }

    private void finalizeTransmission(boolean successful) {
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
        int packetSize = Math.min(algorithm.sizeOfNextPacket(), buffer.remaining());

        byte[] payload = new byte[packetSize];
        buffer.get(payload);

        UtpPacket utpPacket = channel.getNextDataPacket();
        utpPacket.setPayload(payload);
        // Calculate remaining buffer size, capped at MAX_UINT
        int leftInBuffer = (int) Math.min(buffer.remaining(), UnsignedTypesUtil.MAX_UINT & 0xFFFFFFFF);
        utpPacket.setWindowSize(leftInBuffer);
        // Convert UTP packet to bytes and prepare DatagramPacket
        byte[] utpPacketBytes = utpPacket.toByteArray();
        DatagramPacket udpPacket = new DatagramPacket(
                utpPacketBytes,
                utpPacketBytes.length,
                channel.getRemoteAdress()
        );

        // Mark the packet as on the fly
        algorithm.markPacketOnfly(utpPacket, udpPacket);

        return udpPacket;
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