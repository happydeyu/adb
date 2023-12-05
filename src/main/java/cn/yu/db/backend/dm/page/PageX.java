package cn.yu.db.backend.dm.page;

import cn.yu.db.backend.dm.pagecache.PageCache;
import cn.yu.db.utils.ByteUtil;

import java.util.Arrays;

/**
 * @author Yu
 * @description 普通页面以2字节无符号数开始，表示这一页空闲位置的偏移
 * @date 2023-08-08
 */

public class PageX {
    private static final short OFFSET_POS = 0;  //偏移量存储的起始位置
    private static final short OFFSET_DATA_LEN = 2; //偏移量的长度字节
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OFFSET_DATA_LEN;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFreeOffset(raw, OFFSET_DATA_LEN);
        return raw;
    }
    private static void setFreeOffset(byte[] raw, short offset){
        System.arraycopy(ByteUtil.shortToByte(offset), 0, raw, OFFSET_POS, OFFSET_DATA_LEN);
    }

    public static short getFreeOffset(Page pg){
        return getFreeOffset(pg.getData());
    }

    private static short getFreeOffset(byte[] raw){
        return ByteUtil.byteToShort(Arrays.copyOfRange(raw, 0, 2));
    }

    //插入raw到page
    public static short insert(byte[] raw, Page pg){
        short offset = getFreeOffset(pg);
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFreeOffset(pg.getData(), (short) (offset+raw.length));
        return (short) (offset + raw.length);
    }

    public static int getFreeSize(Page pg){
        return PageCache.PAGE_SIZE - getFreeOffset(pg);
    }

    public static void recoverInsert(Page pg, byte[] raw, short offset){
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        short rawFreeOffset = getFreeOffset(pg.getData());
        if(rawFreeOffset < raw.length + offset){
            setFreeOffset(pg.getData(), (short) (raw.length+offset));
        }
    }

    //raw插入到pg的offset位置
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
