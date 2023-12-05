package cn.yu.db.backend.dm;

import cn.yu.db.backend.common.SubArray;
import cn.yu.db.backend.dm.dataitem.DataItem;
import cn.yu.db.backend.dm.log.Logger;
import cn.yu.db.backend.dm.page.Page;
import cn.yu.db.backend.dm.page.PageX;
import cn.yu.db.backend.dm.pagecache.PageCache;
import cn.yu.db.backend.tm.TransactionManager;
import cn.yu.db.utils.ByteUtil;
import cn.yu.db.utils.Panic;
import com.google.common.primitives.Bytes;

import java.util.*;

/**
 * @author Yu
 * @description TODO
 *      updateLog: [LogType] [XID] [UID] [OldRaw] [NewRaw]
 *      insertLog: [LogType] [XID] [Pgno] [Offset] [Raw]
 * @date 2023-08-10
 */

public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OFFSET_TYPE = 0;
    private static final int OFFSET_XID = OFFSET_TYPE+1;
    private static final int OFFSET_UPDATE_UID = OFFSET_XID+8;
    private static final int OFFSET_UPDATE_RAW = OFFSET_UPDATE_UID+8;

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OFFSET_INSERT_PGNO = OFFSET_XID+8;
    private static final int OFFSET_INSERT_OFFSET = OFFSET_INSERT_PGNO+4;
    private static final int OFFSET_INSERT_RAW = OFFSET_INSERT_OFFSET+2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = ByteUtil.longToByte(xid);
        byte[] pgnoRaw = ByteUtil.intToByte(pg.getPageNum());
        byte[] offsetRaw = ByteUtil.shortToByte(PageX.getFreeOffset(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }


    public static void recover(TransactionManager tm, Logger lg, PageCache pc){
        System.out.println("Recovering...");
        int maxPgno = 1;
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) {
                break;
            }
            int pgno;
            if (isInsertLog(log)) {
                InsertLogInfo logInfo = parseInsertLog(log);
                pgno = logInfo.pgno;
            }else {
                UpdateLogInfo logInfo = parseUpdateLogInfo(log);
                pgno = logInfo.pgno;
            }
            if (pgno>maxPgno){
                maxPgno = pgno;
            }
        }
        pc.truncateByPgno(maxPgno);
        System.out.println(" Truncate to "+ maxPgno + " pages");
        redoTransaction(tm, lg, pc);
        undoTransaction(tm, lg, pc);
    }

    private static void redoTransaction(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) {
                break;
            }
            if (isInsertLog(log)) {
                InsertLogInfo info = parseInsertLog(log);
                long xid = info.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            }else {
                UpdateLogInfo info = parseUpdateLogInfo(log);
                long xid = info.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if (flag==REDO) {
            UpdateLogInfo info = parseUpdateLogInfo(log);
            pgno = info.pgno;
            offset = info.offset;
            raw = info.newRaw;
        }else {
            UpdateLogInfo info = parseUpdateLogInfo(log);
            pgno = info.pgno;
            offset = info.offset;
            raw = info.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        PageX.recoverUpdate(pg, raw, offset);
        pg.release();
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo info = parseInsertLog(log);
        Page page = null;
        try {
            page = pc.getPage(info.pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (flag == REDO) {
            DataItem.setDataItemRawInvalid(info.raw);
        }
        PageX.recoverInsert(page, info.raw, info.offset);
        page.release();
    }

    private static void undoTransaction(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) {
                break;
            }
            long xid;
            if (isInsertLog(log)) {
                InsertLogInfo info = parseInsertLog(log);
                xid = info.xid;
            } else {
                UpdateLogInfo info = parseUpdateLogInfo(log);
                xid = info.xid;
            }
            if (tm.isActive(xid)) {
                if (!logCache.containsKey(xid)) {
                    logCache.put(xid, new ArrayList<>());
                }
                logCache.get(xid).add(log);
            }
        }

        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                }else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }

    }


    private static UpdateLogInfo parseUpdateLogInfo(byte[] log) {
        UpdateLogInfo info = new UpdateLogInfo();
        info.xid = ByteUtil.byteToLong(Arrays.copyOfRange(log, OFFSET_XID, OFFSET_UPDATE_UID));
        long uid = ByteUtil.byteToLong(Arrays.copyOfRange(log, OFFSET_UPDATE_UID, OFFSET_UPDATE_RAW));
        info.offset = (short)(uid & ((1L << 16) - 1)); //uid低16位
        uid >>>= 32;    //高32位变为低32位
        info.pgno = (int)(uid & ((1L << 32) - 1));  //原uid的高32位
        int length = (log.length - OFFSET_UPDATE_RAW) / 2;
        info.oldRaw = Arrays.copyOfRange(log, OFFSET_UPDATE_RAW, OFFSET_UPDATE_RAW+length);
        info.newRaw = Arrays.copyOfRange(log, OFFSET_UPDATE_RAW+length, OFFSET_UPDATE_RAW+length*2);
        return info;
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo info = new InsertLogInfo();
        info.xid = ByteUtil.byteToLong(Arrays.copyOfRange(log, OFFSET_XID, OFFSET_INSERT_PGNO));
        info.pgno = ByteUtil.byteToInt(Arrays.copyOfRange(log, OFFSET_INSERT_PGNO, OFFSET_INSERT_OFFSET));
        info.offset = ByteUtil.byteToShort(Arrays.copyOfRange(log, OFFSET_INSERT_OFFSET, OFFSET_INSERT_RAW));
        info.raw = Arrays.copyOfRange(log, OFFSET_INSERT_RAW, log.length);
        return info;
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = ByteUtil.longToByte(xid);
        byte[] uidRaw =ByteUtil.longToByte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }


}
