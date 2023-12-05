package cn.yu.db.backend.dm.dataitem;

import cn.yu.db.backend.common.SubArray;
import cn.yu.db.backend.dm.manager.DataManagerImpl;
import cn.yu.db.backend.dm.page.Page;
import cn.yu.db.utils.ByteUtil;
import cn.yu.db.utils.Types;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-11
 */

public interface DataItem {
    SubArray data();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = ByteUtil.shortToByte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    // 从页面的offset处解析处dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = ByteUtil.byteToShort(Arrays.copyOfRange(raw, offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNum(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset+length), new byte[length], pg, uid, dm);
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }


}
