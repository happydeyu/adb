package cn.yu.db.backend.dm.page;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-07
 */

public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNum();
    byte[] getData();
}
