package net.utp4j.channels.impl.operations;

import net.utp4j.channels.impl.UTPClient;
import net.utp4j.channels.impl.UtpTimestampedPacketDTO;
import net.utp4j.channels.impl.alg.UtpAlgConfiguration;
import net.utp4j.data.MicroSecondsTimeStamp;
import net.utp4j.data.SelectiveAckHeaderExtension;
import net.utp4j.data.UtpPacket;
import net.utp4j.data.bytes.UnsignedTypesUtil;
import org.apache.logging.log4j.Logger;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;


public class UTPReadingFuture {

    private static final int PACKET_DIFF_WARP = 50000;
    private final ByteBuffer buffer;

    private final SkippedPacketBuffer skippedBuffer = new SkippedPacketBuffer();
    private boolean graceFullInterrupt;
    private MicroSecondsTimeStamp timeStamper;
    private long totalPayloadLength = 0;
    private long lastPacketTimestamp;
    private int lastPayloadLength;

    private long nowtimeStamp;
    private long lastPackedRecieved;
    private final long startReadingTimeStamp;
    private boolean gotLastPacket = false;
    // in case we ack every x-th packet, this is the counter.
    private int currentPackedAck = 0;

    private static final Logger LOG = LogManager.getLogger(UTPReadingFuture.class);
    private final UTPClient channel;
    private final CompletableFuture<Void> readFuture;

    public UTPReadingFuture(UTPClient channel, ByteBuffer buff, MicroSecondsTimeStamp timestamp) {
        this.channel = channel;
        this.buffer = buff;
        this.timeStamper = timestamp;
        this.lastPayloadLength = UtpAlgConfiguration.MAX_PACKET_SIZE;
        this.startReadingTimeStamp = timestamp.timeStamp();
        this.readFuture = new CompletableFuture<>();
    }


    public CompletableFuture<Void> startReading() {
        CompletableFuture.runAsync(() -> {
            boolean successful = false;
            IOException exception = null;
            try {
                while (continueReading()) {
                    BlockingQueue<UtpTimestampedPacketDTO> queue = channel.getDataGramQueue();
                    UtpTimestampedPacketDTO packetDTO = queue.poll(UtpAlgConfiguration.TIME_WAIT_AFTER_LAST_PACKET / 2, TimeUnit.MICROSECONDS);
                    nowtimeStamp = timeStamper.timeStamp();
                    if (packetDTO != null) {
                        currentPackedAck++;
                        lastPackedRecieved = packetDTO.stamp();

                        if (isLastPacket(packetDTO)) {
                            gotLastPacket = true;
                            lastPacketTimestamp = timeStamper.timeStamp();
                            LOG.info("Received the last packet.");
                        }

                        if (isPacketExpected(packetDTO.utpPacket())) {
                            handleExpectedPacket(packetDTO);
                        } else {
                            handleUnexpectedPacket(packetDTO);
                        }
                        if (ackThisPacket()) {
                            currentPackedAck = 0;
                        }
                    }

                    /*TODO: How to measure Rtt here for dynamic timeout limit?*/
                    if (isTimedOut()) {
                        if (!hasSkippedPackets()) {
                            gotLastPacket = true;
                            LOG.info("Ending reading, no more incoming data");
                        } else {
                            //LOG.debug("now: " + nowtimeStamp + " last: " + lastPackedRecieved + " = " + (nowtimeStamp - lastPackedRecieved));
                            //LOG.debug("now: " + nowtimeStamp + " start: " + startReadingTimeStamp + " = " + (nowtimeStamp - startReadingTimeStamp));
                            throw new IOException("Timeout occurred with skipped packets.");
                        }
                    }
                    successful = true;
                }
            } catch (IOException | InterruptedException | ArrayIndexOutOfBoundsException e) {
                LOG.debug("Something went wrong during packet processing!");
            } finally {
                channel.returnFromReading();
                if (successful) {
                    readFuture.complete(null);
                } else {
                    readFuture.completeExceptionally(new RuntimeException("Something went wrong!"));
                }
                LOG.info("Buffer position: {}, Buffer limit: {}", buffer.position(), buffer.limit());
                LOG.info("Total payload length: {}", totalPayloadLength);
                LOG.debug("Reader stopped.");
            }
        });
        return this.readFuture;
    }


    private boolean isTimedOut() {
        //TODO: extract constants...
        /* time out after 4sec, when eof not reached */
        boolean timedOut = nowtimeStamp - lastPackedRecieved >= 4000000;
        /* but if remote socket has not recieved synack yet, he will try to reconnect
         * await that aswell */
        boolean connectionReattemptAwaited = nowtimeStamp - startReadingTimeStamp >= 4000000;
        return timedOut && connectionReattemptAwaited;
    }

    private boolean isLastPacket(UtpTimestampedPacketDTO timestampedPair) {
//		log.debug("WindowSize: " + (timestampedPair.utpPacket().getWindowSize() & 0xFFFFFFFF));
//		log.debug("got Packet: " + (timestampedPair.utpPacket().getSequenceNumber() & 0xFFFF) + " and will ack? " + ackThisPacket() + " acksizeCounter " + currentPackedAck);
        return (timestampedPair.utpPacket().getWindowSize() & 0xFFFFFFFF) == 0;
    }

    private void handleExpectedPacket(UtpTimestampedPacketDTO timestampedPair) throws IOException {
//		log.debug("handling expected packet: " + (timestampedPair.utpPacket().getSequenceNumber() & 0xFFFF));
        if (hasSkippedPackets()) {
            buffer.put(timestampedPair.utpPacket().getPayload());
            int payloadLength = timestampedPair.utpPacket().getPayload().length;
            lastPayloadLength = payloadLength;
            totalPayloadLength += payloadLength;
            Queue<UtpTimestampedPacketDTO> packets = skippedBuffer.getAllUntillNextMissing();
            int lastSeqNumber = 0;
            if (packets.isEmpty()) {
                lastSeqNumber = timestampedPair.utpPacket().getSequenceNumber() & 0xFFFF;
            }
            UtpPacket lastPacket = null;
            for (UtpTimestampedPacketDTO p : packets) {
                buffer.put(p.utpPacket().getPayload());
                payloadLength += p.utpPacket().getPayload().length;
                lastSeqNumber = p.utpPacket().getSequenceNumber() & 0xFFFF;
                lastPacket = p.utpPacket();
            }
            skippedBuffer.reindex(lastSeqNumber);
            channel.setCurrentAckNumber(lastSeqNumber);
            //if still has skipped packets, need to selectively ack
            if (hasSkippedPackets()) {
                if (ackThisPacket()) {
//					log.debug("acking expected, had, still have");
                    SelectiveAckHeaderExtension headerExtension = skippedBuffer.createHeaderExtension();
                    channel.selectiveAckPacket(headerExtension, getTimestampDifference(timestampedPair), getLeftSpaceInBuffer());
                }

            } else {
                if (ackThisPacket()) {
//					log.debug("acking expected, has, than has nomore");
                    channel.ackPacket(lastPacket, getTimestampDifference(timestampedPair), getLeftSpaceInBuffer());
                }
            }
        } else {
            if (ackThisPacket()) {
//				log.debug("acking expected, nomore");
                channel.ackPacket(timestampedPair.utpPacket(), getTimestampDifference(timestampedPair), getLeftSpaceInBuffer());
            } else {
                channel.setCurrentAckNumber(timestampedPair.utpPacket().getSequenceNumber() & 0xFFFF);
            }
            buffer.put(timestampedPair.utpPacket().getPayload());
            totalPayloadLength += timestampedPair.utpPacket().getPayload().length;
        }
    }

    private boolean ackThisPacket() {
        return currentPackedAck >= UtpAlgConfiguration.SKIP_PACKETS_UNTIL_ACK;
    }

    public long getLeftSpaceInBuffer() throws IOException {
        return (long) (skippedBuffer.getFreeSize()) * lastPayloadLength;
    }

    private int getTimestampDifference(UtpTimestampedPacketDTO timestampedPair) {
        return timeStamper.utpDifference(timestampedPair.utpTimeStamp(), timestampedPair.utpPacket().getTimestamp());
    }

    private void handleUnexpectedPacket(UtpTimestampedPacketDTO timestampedPair) throws IOException {
        int expected = getExpectedSeqNr();
//		log.debug("handling unexpected packet: " + (timestampedPair.utpPacket().getSequenceNumber() & 0xFFFF));
        int seqNr = timestampedPair.utpPacket().getSequenceNumber() & 0xFFFF;
        if (skippedBuffer.isEmpty()) {
            skippedBuffer.setExpectedSequenceNumber(expected);
        }
        //TODO: wrapping seq nr: expected can be 5 e.g.
        // but buffer can recieve 65xxx, which already has been acked, since seq numbers wrapped.
        // current implementation puts this wrongly into the buffer. it should go in the else block
        // possible fix: alreadyAcked = expected > seqNr || seqNr - expected > CONSTANT;
        boolean alreadyAcked = expected > seqNr || seqNr - expected > PACKET_DIFF_WARP;

        boolean saneSeqNr = expected == skippedBuffer.getExpectedSequenceNumber();
//		log.debug("saneSeqNr: " + saneSeqNr + " alreadyAcked: " + alreadyAcked + " will ack: " + ackThisPacket());
        if (saneSeqNr && !alreadyAcked) {
            skippedBuffer.bufferPacket(timestampedPair);
            // need to create header extension after the packet is put into the incomming buffer.
            SelectiveAckHeaderExtension headerExtension = skippedBuffer.createHeaderExtension();
            if (ackThisPacket()) {
//				log.debug("acking unexpected snae");
                channel.selectiveAckPacket(headerExtension, getTimestampDifference(timestampedPair), getLeftSpaceInBuffer());
            }
        } else if (ackThisPacket()) {
            SelectiveAckHeaderExtension headerExtension = skippedBuffer.createHeaderExtension();
//			log.debug("acking unexpected  nonsane");
            channel.ackAlreadyAcked(headerExtension, getTimestampDifference(timestampedPair), getLeftSpaceInBuffer());
        }
    }

    public boolean isPacketExpected(UtpPacket utpPacket) {
        int seqNumberFromPacket = utpPacket.getSequenceNumber() & 0xFFFF;
//		log.debug("Expected Sequence Number == " + getExpectedSeqNr());
        return getExpectedSeqNr() == seqNumberFromPacket;
    }

    private int getExpectedSeqNr() {
        int ackNumber = channel.getCurrentAckNumber();
        if (ackNumber == UnsignedTypesUtil.MAX_USHORT) {
            return 1;
        }
        return ackNumber + 1;
    }

    public void graceFullInterrupt() {
        this.graceFullInterrupt = true;
    }


    //done
    private boolean continueReading() {
        return !graceFullInterrupt && (!gotLastPacket || hasSkippedPackets() || !timeAwaitedAfterLastPacket());
    }

    private boolean hasSkippedPackets() {
        return !skippedBuffer.isEmpty();
    }

    private boolean timeAwaitedAfterLastPacket() {
        return (timeStamper.timeStamp() - lastPacketTimestamp) > UtpAlgConfiguration.TIME_WAIT_AFTER_LAST_PACKET
                && gotLastPacket;
    }

    public boolean isAlive() {
        return this.readFuture.isDone();
    }
}
