package net.utp4j.data.bytes;


import net.utp4j.data.bytes.exceptions.ByteOverflowException;
import net.utp4j.data.bytes.exceptions.SignedNumberException;


public final class UnsignedTypesUtil {

    public static final long MAX_UBYTE = 255;
    public static final long MAX_SEQUENCE_NR = 65535;
    public static final long MAX_UINT = 4294967295L;


    public static byte longToUbyte(long longvalue) {
        if (longvalue > MAX_UBYTE) {
            throw new ByteOverflowException(getExceptionText(MAX_UBYTE, longvalue));
        } else if (longvalue < 0) {
            throw new SignedNumberException(getExceptionText(MAX_UBYTE, longvalue));
        }
        return (byte) (longvalue & 0xFF);
    }

    public static short longToUshort(long longvalue) {
        if (longvalue > MAX_SEQUENCE_NR) {
            throw new ByteOverflowException(getExceptionText(MAX_SEQUENCE_NR, longvalue));
        } else if (longvalue < 0) {
            throw new SignedNumberException(getExceptionText(MAX_SEQUENCE_NR, longvalue));
        }
        return (short) (longvalue & 0xFFFF);
    }

    public static int longToUint(long longvalue) {
        if (longvalue > MAX_UINT) {
            throw new ByteOverflowException(getExceptionText(MAX_UINT, longvalue));
        } else if (longvalue < 0) {
            throw new SignedNumberException(getExceptionText(MAX_UINT, longvalue));
        }
        return (int) (longvalue & 0xFFFFFFFF);
    }

    private static String getExceptionText(long max, long actual) {
        return "Cannot convert to unsigned type. " +
                "Possible values [0, " + max + "] but got " + actual + ".";
    }

    public static short bytesToUshort(byte first, byte second) {
        return (short) (((first & 0xFF) << 8) | (second & 0xFF));
    }

    public static int bytesToUint(byte first, byte second, byte third, byte fourth) {
        int firstI = (first & 0xFF) << 24;
        int secondI = (second & 0xFF) << 16;
        int thirdI = (third & 0xFF) << 8;
        int fourthI = (fourth & 0xFF);

        return firstI | secondI | thirdI | fourthI;


    }


}
