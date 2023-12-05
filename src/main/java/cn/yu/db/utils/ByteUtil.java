package cn.yu.db.utils;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-06
 */

public class ByteUtil {
    public static byte[] shortToByte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    public static short byteToShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    public static byte[] intToByte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    public static int byteToInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    public static long byteToLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    public static byte[] longToByte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static ParseStringRes byteToString(byte[] raw) {
        int length = byteToInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4+length));
        return new ParseStringRes(str, length+4);
    }

    public static byte[] stringToByte(String str) {
        byte[] l = intToByte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static long strToUid(String key) {
        long seed = 13331;
        long res = 0;
        for(byte b : key.getBytes()) {
            res = res * seed + (long)b;
        }
        return res;
    }
}
