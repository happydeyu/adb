package cn.yu.db.backend.dm.manager;

import cn.yu.db.backend.dm.Recover;
import cn.yu.db.backend.dm.dataitem.DataItem;
import cn.yu.db.backend.dm.log.Logger;
import cn.yu.db.backend.dm.page.PageOne;
import cn.yu.db.backend.dm.pagecache.PageCache;
import cn.yu.db.backend.tm.TransactionManager;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-11
 */

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }

    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
