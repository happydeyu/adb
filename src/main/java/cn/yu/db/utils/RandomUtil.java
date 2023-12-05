package cn.yu.db.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-08
 */

public class RandomUtil {
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
