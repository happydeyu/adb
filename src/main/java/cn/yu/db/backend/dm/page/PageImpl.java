package cn.yu.db.backend.dm.page;

import cn.yu.db.backend.dm.pagecache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-07
 */

public class PageImpl implements Page{
    private int pageNum;
    private byte[] data;
    private boolean dirty;
    private Lock lock;
    private PageCache pc;

    public PageImpl(int pageNum, byte[] data, PageCache pc) {
        this.pageNum = pageNum;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        this.lock.lock();
    }

    @Override
    public void unlock() {
        this.lock.unlock();
    }

    @Override
    public void release() {
        this.pc.release(this);
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNum() {
        return pageNum;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
