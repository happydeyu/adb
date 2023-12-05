package cn.yu.db.backend.vm;

import cn.yu.db.backend.dm.manager.DataManager;
import cn.yu.db.backend.tm.TransactionManager;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-12
 */

public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }
}
