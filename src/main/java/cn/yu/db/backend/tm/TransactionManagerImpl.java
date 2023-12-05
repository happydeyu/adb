package cn.yu.db.backend.tm;

import cn.yu.db.utils.ByteUtil;
import cn.yu.db.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yu
 * @description TODO
 * @date 2023-07-26
 */

public class TransactionManagerImpl implements TransactionManager{
    // XID文件头长度
    public static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;

    // 超级事务，永远为committed状态
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile file, FileChannel fc) {
        this.file = file;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXidCounter();
    }
    //检查XID是否合法
    private void checkXidCounter() {
        long fileLength = 0;
        try {
            fileLength = this.file.length();
        } catch (IOException e) {
            new RuntimeException("XID异常").printStackTrace();
        }
        if(fileLength < LEN_XID_HEADER_LENGTH){
            new RuntimeException("XID异常").printStackTrace();
        }
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.xidCounter = ByteUtil.byteToLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLength){
            new RuntimeException("XID异常").printStackTrace();
        }
    }

    //根据xid获取在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }
    // 将XID加一，并更新XID Header
    private void incrXidCounter() {
        xidCounter ++;
        ByteBuffer buf = ByteBuffer.wrap(ByteUtil.longToByte((xidCounter)));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }



    public boolean checkXid(long xid, byte status){
        long offset = getXidPosition(xid);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buffer);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buffer.array()[0] == status;
    }

    public void updateXid(long xid, byte status){
        long offset = getXidPosition(xid);
        byte[] bytes = new byte[XID_FIELD_SIZE];
        bytes[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        try {
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }


    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = this.xidCounter + 1;
            updateXid(xid, FIELD_TRAN_ACTIVE);
            incrXidCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        updateXid(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        updateXid(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isActive(long xid) {
        return xid == SUPER_XID? false: checkXid(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        return xid == SUPER_XID? true: checkXid(xid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        return xid == SUPER_XID? false: checkXid(xid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }

    }
}
