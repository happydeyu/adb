package cn.yu.db.backend.dm.pagecache;

import cn.yu.db.backend.dm.page.Page;
import cn.yu.db.common.DbException;
import cn.yu.db.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-07
 */

public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13; //8k



    int newPage(byte[] initData);
    Page getPage(int pageNum) throws Exception;
    void close();
    void release(Page page);

    void truncateByPgno(int maxPageNum);
    int getPageNum();
    void flushPage(Page pg);

    public static PageCacheImpl create(String path, long memory){
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(DbException.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(DbException.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl((int)memory/PAGE_SIZE, raf, fc);
    }

    public static PageCacheImpl open(String path, long mem) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(DbException.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(DbException.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl((int)mem/PAGE_SIZE, raf, fc);
    }
}
