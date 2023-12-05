package cn.yu.db.backend.dm.log;

import cn.yu.db.common.DbException;
import cn.yu.db.utils.ByteUtil;
import cn.yu.db.utils.Panic;
import com.google.common.primitives.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yu
 * @description 日志文件 [checksum 4字节][log1][log2][...][logN][BadTail]
 *              日志logX格式 [size 4字节][checksum][data]
 * @date 2023-08-08
 */

public class LoggerImpl implements Logger{

    private static final int SEED = 13331;

    private static final int OFFSET_SIZE = 0;
    private static final int OFFSET_CHECKSUM = OFFSET_SIZE + 4;
    private static final int OFFSET_DATA = OFFSET_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;  //log文件的checksum

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init(){
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.fileSize = size;
        this.xChecksum = ByteUtil.byteToInt(raw.array());

        checkAndRemoveTail();
    }

    private void checkAndRemoveTail() {
        rewind();
        int xCheck = 0;
        while (true){
            byte[] log = internNext();
            if (log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if (xCheck != xChecksum){
            Panic.panic(DbException.BadLogFileException);
        }
        truncate(position);
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    private byte[] internNext() {
        if (position+OFFSET_DATA>=fileSize) {
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = ByteUtil.byteToInt(tmp.array());

        if (position + size + OFFSET_DATA > fileSize){
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate(OFFSET_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        byte[] log = buf.array();
        int checksum = calChecksum(0, Arrays.copyOfRange(log, OFFSET_DATA, log.length));
        int checksum1 = ByteUtil.byteToInt(Arrays.copyOfRange(log, OFFSET_CHECKSUM, OFFSET_DATA));
        if (checksum != checksum1) {
            return null;
        }
        position += log.length;
        return log;
    }


    //计算单条日志的checksum
    private int calChecksum(int xCheck, byte[] data){
        for (byte d : data) {
            xCheck = xCheck * SEED + d;
        }
        return xCheck;
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(ByteUtil.intToByte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = ByteUtil.intToByte(calChecksum(0, data));
        byte[] size = ByteUtil.intToByte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) {
        lock.lock();
        try {
            fc.truncate(x); //截断文件，超过position长度的将会舍弃
        } catch (Exception e){
            Panic.panic(e);
        } finally{
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) {
                return null;
            }
            return Arrays.copyOfRange(log, OFFSET_DATA, log.length);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
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
