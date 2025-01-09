package net.utp4j.data;

import net.utp4j.data.bytes.UnsignedTypesUtil;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Objects;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt32;

import static net.utp4j.data.UtpPacketUtils.DEF_HEADER_LENGTH;


/**
 * 0       4       8               16              24              32
 * +-------+-------+---------------+---------------+---------------+
 * | type  | ver   | extension     | connection_id                 |
 * +-------+-------+---------------+---------------+---------------+
 * | timestamp_microseconds                                        |
 * +---------------+---------------+---------------+---------------+
 * | timestamp_difference_microseconds                             |
 * +---------------+---------------+---------------+---------------+
 * | wnd_size                                                      |
 * +---------------+---------------+---------------+---------------+
 * | seq_nr                        | ack_nr                        |
 * +---------------+---------------+---------------+---------------+
 */
public class UtpPacket {

    private byte typeVersion;
    private Bytes firstExtension;

    //private final Bytes version = VERSION;

    private Bytes connectionId;
    private UInt32 timestamp;
    private UInt32 timestampDifference;
    private UInt32 windowSize;
    private Bytes sequenceNumber;
    private Bytes ackNumber;

    private UtpHeaderExtension[] extensions;
    private Bytes payload;


    public UtpPacket(byte typeVersion, Bytes connectionId, UInt32 timestamp,
                     UInt32 timestampDifference, UInt32 windowSize, Bytes sequenceNumber, Bytes ackNumber) {
        this.typeVersion = typeVersion;
        this.connectionId = connectionId;
        this.timestamp = timestamp;
        this.timestampDifference = timestampDifference;
        this.windowSize = windowSize;
        this.sequenceNumber = sequenceNumber;
        this.ackNumber = ackNumber;
        this.payload = Bytes.EMPTY;
    }

    public UtpPacket(){

    }


    public Bytes getPayload() {
        return payload;
    }

    public void setPayload(Bytes payload) {
        this.payload = payload;
    }

    public void setExtensions(UtpHeaderExtension[] extensions) {
        this.extensions = extensions;
    }

    public UtpHeaderExtension[] getExtensions() {
        return this.extensions;
    }

    public Bytes getFirstExtension() {
        return firstExtension;
    }

    public void setFirstExtension(Bytes firstExtension) {
        this.firstExtension = firstExtension;
    }

    public byte getTypeVersion() {
        return typeVersion;
    }

    public void setTypeVersion(byte typeVersion) {
        this.typeVersion = typeVersion;
    }

    public Bytes getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(Bytes connectionId) {
        this.connectionId = connectionId;
    }

    public UInt32 getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(UInt32 timestamp) {
        this.timestamp = timestamp;
    }

    public UInt32 getTimestampDifference() {
        return timestampDifference;
    }

    public void setTimestampDifference(int timestampDifference) {
        this.timestampDifference = UInt32.valueOf(timestampDifference);
    }

    public UInt32 getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(UInt32 windowSize) {
        this.windowSize = windowSize;
    }

    public Bytes getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(Bytes sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public Bytes getAckNumber() {
        return ackNumber;
    }

    public void setAckNumber(Bytes ackNumber) {
        this.ackNumber = ackNumber;
    }


    public Bytes toByteArray() {
        if (!hasExtensions()) {
            return Bytes.concatenate(getExtensionlessBytes(), this.payload);
        }


        Bytes header = getExtensionlessBytes();

        Bytes finalExtensions = Bytes.EMPTY;
        for (UtpHeaderExtension extension : this.extensions) {
            Bytes extensionInBytes = Bytes.wrap(extension.toByteArray());
            finalExtensions = Bytes.concatenate(finalExtensions, extensionInBytes);
        }
        return Bytes.concatenate(header, finalExtensions, this.payload);
    }


    private Bytes getExtensionlessBytes() {
        return Bytes.concatenate(
                Bytes.of(typeVersion),
                firstExtension,
                connectionId,
                timestamp.toBytes(),
                timestampDifference.toBytes(),
                windowSize.toBytes(),
                sequenceNumber,
                ackNumber);
    }

    //todo refactor
    private boolean hasExtensions() {
        return !((extensions == null || extensions.length == 0) && firstExtension.isZero());
    }

    private int getTotalLengthOfExtensions() {
        if (!hasExtensions()) return 0;

        int length = 0;
        if (extensions != null) {
            for (UtpHeaderExtension extension : extensions) {
                length += 2 + extension.getBitMask().length;
            }
        }
        return length;
    }

    public int getPacketLength() {
        return DEF_HEADER_LENGTH + getTotalLengthOfExtensions() + this.payload.size();
    }

    public void setFromByteArray(byte[] array, int length, int offset) {
        if (array == null) {
            return;
        }
        typeVersion = array[0];
        firstExtension = Bytes.of(array[1]);
        connectionId =  Bytes.of(array[2], array[3]);
        timestamp =  UInt32.fromBytes(Bytes.of(array[4], array[5], array[6], array[7]));
        timestampDifference =  UInt32.fromBytes(Bytes.of(array[8], array[9], array[10], array[11]));
        windowSize =UInt32.fromBytes(Bytes.of(array[12], array[13], array[14], array[15]));
        sequenceNumber = Bytes.of(array[16], array[17]);
        ackNumber = Bytes.of(array[18], array[19]);

        int utpOffset = offset + UtpPacketUtils.DEF_HEADER_LENGTH;
        if (!firstExtension.isZero()) {
            utpOffset += loadExtensions(array);
        }
        this.payload =  Bytes.wrap(array, utpOffset, length - utpOffset);
    }


    private int loadExtensions(byte[] array) {
        byte extensionType = array[1];
        int extensionStartIndex = 20;
        int totalLength = 0;

        ArrayList<UtpHeaderExtension> list = new ArrayList<UtpHeaderExtension>();
        UtpHeaderExtension extension = UtpHeaderExtension.resolve(extensionType);

        while (extension != null) {
            int extensionLength = array[extensionStartIndex + 1] & 0xFF;
            byte[] bitmask = new byte[extensionLength];
            System.arraycopy(array, extensionStartIndex + 2, bitmask, 0, extensionLength);
            extension.setNextExtension(array[extensionStartIndex]);
            extension.setBitMask(bitmask);
            list.add(extension);
            totalLength = extensionLength + 2;
            int nextPossibleExtensionIndex = extensionLength + 2 + extensionStartIndex;
            // packet is enough big
            if (array.length > nextPossibleExtensionIndex) {
                extension = UtpHeaderExtension.resolve(array[nextPossibleExtensionIndex]);
                extensionStartIndex = nextPossibleExtensionIndex;
            } else { // packet end reached.
                extension = null;
            }
        }
        UtpHeaderExtension[] extensions = list.toArray(new UtpHeaderExtension[list.size()]);
        this.extensions = extensions;
        return totalLength;
    }


    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder();
        ret.append(String.format("[Type: %d] ", typeVersion));
        ret.append(String.format("[FirstExt: %s] ", firstExtension.toHexString()));
        ret.append(String.format("[ConnId: %s] ", connectionId.toHexString()));
        ret.append(String.format("[Wnd: %s] ", windowSize.toHexString()));
        ret.append(String.format("[Seq: %s] ", sequenceNumber.toHexString()));
        ret.append(String.format("[Ack: %s] ", ackNumber.toHexString()));
        if (extensions != null) {
            for (int i = 0; i < extensions.length; i++) {
                ret.append("[Ext_").append(i).append(": ").append(extensions[i].getNextExtension() & 0xFF).append(" ").append(extensions[i].getLength()).append("] ");
            }
        }
        return ret.toString();
    }

    public static DatagramPacket createDatagramPacket(UtpPacket packet, SocketAddress socketAddress) throws IOException {
        Bytes utpPacketBytes = packet.toByteArray();
        int length = packet.getPacketLength();
        return new DatagramPacket(utpPacketBytes.toArray(), length, socketAddress);
    }
}
