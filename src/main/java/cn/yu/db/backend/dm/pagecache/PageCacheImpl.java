package cn.yu.db.backend.dm.pagecache;

import cn.yu.db.backend.common.AbstractCache;
import cn.yu.db.backend.dm.page.Page;
import cn.yu.db.backend.dm.page.PageImpl;
import cn.yu.db.common.DbException;
import cn.yu.db.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-08
 */

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNums;

    public PageCacheImpl(int maxResource, RandomAccessFile raf, FileChannel fc) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM){
            Panic.panic(DbException.MemTooSmall);
        }
        long len = 0;
        try {

            len = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.raf = raf;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNums = new AtomicInteger((int) (len/PAGE_SIZE));
    }


    @Override
    protected Page getForCache(long key) throws Exception {
        long offset = pageOffset((int) key);
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();

        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();

        return new PageImpl((int) key, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if (pg.isDirty()) {
            flushPage(pg);
            pg.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNums.incrementAndGet();
        Page page = new PageImpl(pgno, initData, null);
        flushPage(page);
        return pgno;
    }

    @Override
    public Page getPage(int pageNum) throws Exception {
        return get(pageNum);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void release(Page page) {
        release(page.getPageNum());
    }

    //截断文件
    @Override
    public void truncateByPgno(int maxPageNum) {
        long size = pageOffset(maxPageNum + 1);
        try {
            raf.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNums.set(maxPageNum);
    }

    @Override
    public int getPageNum() {
        return pageNums.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        int pgno = pg.getPageNum();
        long offset = pageOffset(pgno);
        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }

    }

    private static long pageOffset(int pgno){
        //页号从1开始
        return (pgno-1)*PAGE_SIZE;
    }
}
