package cn.yu.db.backend.dm.manager;

import cn.yu.db.backend.common.AbstractCache;
import cn.yu.db.backend.dm.Recover;
import cn.yu.db.backend.dm.dataitem.DataItem;
import cn.yu.db.backend.dm.dataitem.DataItemImpl;
import cn.yu.db.backend.dm.log.Logger;
import cn.yu.db.backend.dm.page.Page;
import cn.yu.db.backend.dm.page.PageOne;
import cn.yu.db.backend.dm.page.PageX;
import cn.yu.db.backend.dm.pagecache.PageCache;
import cn.yu.db.backend.dm.pageindex.PageIndex;
import cn.yu.db.backend.dm.pageindex.PageInfo;
import cn.yu.db.backend.tm.TransactionManager;
import cn.yu.db.common.DbException;
import cn.yu.db.utils.Panic;
import cn.yu.db.utils.Types;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-11
 */

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }


    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl) super.get(uid);
        if (!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw DbException.DataTooLargeException;
        }
        PageInfo pi=null;
        for (int i = 0; i < 5; i++) {
            pi = pIndex.select(raw.length);
            if (pi!=null) {
                break;
            }else{
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if (pi==null) {
            throw DbException.DatabaseBusyException;
        }
        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            short offset = PageX.insert(raw, pg);
            pg.release();
            return Types.addressToUid(pi.pgno, offset);
        } finally {
            if (pg!=null) {
                pIndex.add(pi.pgno, PageX.getFreeOffset(pg));
            }else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }

    }

    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    //在创建文件时初始化PageOne
    public void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        try {
            pageOne = pc.getPage(pgno);

        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);

    }

    //打开已有文件时读入PageOne，并验证正确性
    public boolean loadCheckPageOne() {

        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di){
        super.release(di.getUid());
    }

    //初始化pageIndex
    public void fillPageIndex() {
        int pageNum = pc.getPageNum();
        for (int i = 2; i <= pageNum; i++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNum(), PageX.getFreeSize(pg)) ;
            pg.release();
        }
    }
}
