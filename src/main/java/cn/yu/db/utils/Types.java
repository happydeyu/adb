package cn.yu.db.utils;

/**
 * @author Yu
 * @description uid高32位是pgno，低32位是offset
 * @date 2023-08-11
 */

public class Types {
    public static long addressToUid(int pgno, short offset) {
        long u0 = (long)pgno;
        long u1 = (long)offset;
        return u0 << 32 | u1;
    }
}
