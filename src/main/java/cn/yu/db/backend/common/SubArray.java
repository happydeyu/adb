package cn.yu.db.backend.common;

/**
 * @author Yu
 * @description 限制可操作区域数组，模拟共享内存
 * @date 2023-08-07
 */

public class SubArray {
    public byte raw[];
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }

}
