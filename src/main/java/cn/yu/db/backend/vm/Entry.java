package cn.yu.db.backend.vm;

import cn.yu.db.backend.common.SubArray;
import cn.yu.db.backend.dm.dataitem.DataItem;
import cn.yu.db.utils.ByteUtil;
import com.google.common.primitives.Bytes;

import java.util.Arrays;

/**
 * @author Yu
 * @description vm向上层抽象出Entry，Entry的结构：[XMIN][XMAX][DATA]
 * @date 2023-08-12
 */

public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN+8;
    private static final int OF_DATA = OF_XMAX+8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = ByteUtil.longToByte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void remove() {
        dataItem.release();
    }

    public byte[] data(){
        dataItem.rLock();
        try {
            SubArray d = dataItem.data();
            byte[] data = new byte[d.end - d.start - OF_DATA];
            System.arraycopy(d, d.start+OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public void release(){
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray d = dataItem.data();
            return ByteUtil.byteToLong(Arrays.copyOfRange(d.raw, d.start+OF_XMIN, d.start+OF_DATA));
        } finally {

            dataItem.rUnLock();
        }

    }
    public long getUid(){
        return uid;
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(ByteUtil.longToByte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }
}
