package cn.yu.db.backend.tm;

import cn.yu.db.common.DbException;
import cn.yu.db.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Yu
 * @description TODO
 * @date 2023-07-26
 */

public interface TransactionManager {
    //开启事务
    long begin();
    //提交
    void commit(long xid);
    void abort(long xid);
    //是否正在进行
    boolean isActive(long xid);

    //是否提交
    boolean isCommitted(long xid);

    //是否取消
    boolean isAborted(long xid);

    //关闭TM
    void close();

    public static TransactionManagerImpl create(String path){
        File file = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if (!file.createNewFile()) {
                Panic.panic(DbException.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(DbException.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (Exception e) {
            Panic.panic(e);
        }
        //空xid文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }
    public static TransactionManagerImpl open(String path){
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()){
            Panic.panic(DbException.FileNotExistsException);
        }
        if (!f.canRead() || !f.canWrite()) {
            Panic.panic(DbException.FileCannotRWException);
        }
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        FileChannel fc = raf.getChannel();
        return new TransactionManagerImpl(raf, fc);

    }


}
