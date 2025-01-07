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
package net.utp4j.channels.impl.write;

import net.utp4j.channels.impl.UTPClient;
import net.utp4j.channels.impl.UtpTimestampedPacketDTO;
import net.utp4j.channels.impl.alg.UtpAlgorithm;
import net.utp4j.data.MicroSecondsTimeStamp;
import net.utp4j.data.UtpPacket;
import net.utp4j.data.bytes.UnsignedTypesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final static Logger log = LoggerFactory.getLogger(UtpWritingRunnable.class);

    public UtpWritingRunnable(UTPClient channel, ByteBuffer buffer,
                              MicroSecondsTimeStamp timeStamper, CompletableFuture<Void>  writerFuture) {
        this.buffer = buffer;
        this.channel = channel;
        this.timeStamper = timeStamper;
        this.writerFuture = writerFuture;
        algorithm = new UtpAlgorithm(timeStamper, channel.getRemoteAdress());
    }


    @Override
    public void run() {
        algorithm.initiateAckPosition(channel.getCurrentSequenceNumber());
        algorithm.setTimeStamper(timeStamper);
        algorithm.setByteBuffer(buffer);
        isRunning = true;
        IOException possibleExp = null;
        boolean exceptionOccured = false;
        buffer.flip();
        int durchgang = 0;
        int bytesWritten;
        while (continueSending()) {
            try {
                if (!checkForAcks()) {
                    graceFullInterrupt = true;
                    break;
                }

                Queue<DatagramPacket> packetsToResend = algorithm.getPacketsToResend();
                for (DatagramPacket datagramPacket : packetsToResend) {
                    datagramPacket.setSocketAddress(channel.getRemoteAdress());
                    channel.sendPacket(datagramPacket);
                }

            } catch (IOException exp) {
                exp.printStackTrace();
                graceFullInterrupt = true;
                possibleExp = exp;
                exceptionOccured = true;
                break;
            }
            if (algorithm.isTimedOut()) {
                graceFullInterrupt = true;
                possibleExp = new IOException("timed out");
                log.debug("timed out");
                exceptionOccured = true;
            }
//			if(!checkForAcks()) {
//				graceFullInterrupt = true;
//				break;
//			}
            while (algorithm.canSendNextPacket() && !exceptionOccured && !graceFullInterrupt && buffer.hasRemaining()) {
                try {
                    channel.sendPacket(getNextPacket());
                } catch (IOException exp) {
                    exp.printStackTrace();
                    graceFullInterrupt = true;
                    possibleExp = exp;
                    exceptionOccured = true;
                    break;
                }
            }
//			if (!buffer.hasRemaining() && !finSend) {
//				UtpPacket fin = channel.getFinPacket();
//				log.debug("Sending FIN");
//				try {
//					channel.finalizeConnection(fin);
//					algorithm.markFinOnfly(fin);
//				} catch (IOException exp) {
//					exp.printStackTrace();
//					graceFullInterrupt = true;
//					possibleExp = exp;
//					exceptionOccured = true;
//				}
//				finSend = true;
//			}

            durchgang++;
            if (durchgang % 1000 == 0) {
                log.debug("buffer position: " + buffer.position() + " buffer limit: " + buffer.limit());
            }
        }

        if (possibleExp != null) {
            exceptionOccured(possibleExp);
        }
        isRunning = false;
        algorithm.end(buffer.position(), !exceptionOccured);

        log.debug("WRITER OUT");
        channel.removeWriter();

        writerFuture.complete(null);
    }



    private boolean checkForAcks() {
        BlockingQueue<UtpTimestampedPacketDTO> queue = channel.getDataGramQueue();
        try {
            waitAndProcessAcks(queue);
        } catch (InterruptedException ie) {
            return false;
        }
        return true;
    }

    private void waitAndProcessAcks(BlockingQueue<UtpTimestampedPacketDTO> queue) throws InterruptedException {
        long waitingTimeMicros = algorithm.getWaitingTimeMicroSeconds();
        UtpTimestampedPacketDTO temp = queue.poll(waitingTimeMicros, TimeUnit.MICROSECONDS);
        if (temp != null) {
            algorithm.ackRecieved(temp);
            algorithm.removeAcked();
            if (queue.peek() != null) {
                processAcks(queue);
            }
        }
    }

    private void processAcks(BlockingQueue<UtpTimestampedPacketDTO> queue) throws InterruptedException {
        UtpTimestampedPacketDTO pair;
        while ((pair = queue.poll()) != null) {
            algorithm.ackRecieved(pair);
            algorithm.removeAcked();
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


    private void exceptionOccured(IOException exp) {
        possibleException = exp;
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
