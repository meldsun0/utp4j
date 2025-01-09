package net.utp4j.data;

import net.utp4j.data.bytes.UnsignedTypesUtil;
import net.utp4j.data.bytes.exceptions.ByteOverflowException;
import org.apache.tuweni.units.bigints.UInt32;

import static net.utp4j.data.bytes.UnsignedTypesUtil.MAX_UINT;
import static net.utp4j.data.bytes.UnsignedTypesUtil.longToUint;

/**
 * Implements micro second accuracy timestamps for uTP headers and to measure time elapsed.
 *
 * @author Ivan Iljkic (i.iljkic@gmail.com)
 */
public class MicroSecondsTimeStamp {

    private static final long initDateMillis = System.currentTimeMillis();
    private static final long startNs = System.nanoTime();

    /**
     * Returns a uTP time stamp for packet headers.
     *
     * @return timestamp
     */
    public UInt32 utpTimeStamp() {
        int returnStamp;
        //TODO: if performance issues, try bitwise & operator since constant MAX_UINT equals 0xFFFFFF
        // (see http://en.wikipedia.org/wiki/Modulo_operation#Performance_issues )
        long stamp = timeStamp() % UnsignedTypesUtil.MAX_UINT;
        try {
            returnStamp = longToUint(stamp);
        } catch (ByteOverflowException exp) {
            stamp = stamp % MAX_UINT;
            returnStamp = longToUint(stamp);
        }
        return UInt32.valueOf(returnStamp);
    }

    /**
     * Calculates the Difference of the uTP timestamp between now and parameter.
     *
     * @param othertimestamp timestamp
     * @return difference
     */
    public UInt32 utpDifference(UInt32 othertimestamp) {
        return utpTimeStamp().subtract(othertimestamp);

    }


    /**
     * @return timestamp with micro second resulution
     */
    public long timeStamp() {

        long currentNs = System.nanoTime();
        long deltaMs = (currentNs - startNs) / 1000;
        return (initDateMillis * 1000 + deltaMs);
    }

}
