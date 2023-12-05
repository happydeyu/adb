package cn.yu.db.common;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-06
 */

public class DbException {
    public static final Exception BadXidException = new RuntimeException("XID 不正确");
    public static final Exception FileExistsException = new RuntimeException("文件存在");
    public static final Exception FileCannotRWException = new RuntimeException("文件不能读或写异常");


    public static Exception FileNotExistsException = new RuntimeException("文件不存在");
    public static Exception CacheFullException = new RuntimeException("缓存满了");
    public static Exception MemTooSmall = new RuntimeException("内存太小了");
    public static Exception BadLogFileException = new RuntimeException("日志文件错误");
    public static Exception DeadLockException = new RuntimeException("死锁");
    public static Exception NullEntryException = new RuntimeException("Entry 为null");
    public static Exception DataTooLargeException = new RuntimeException("数据太大");
    public static Exception DatabaseBusyException;
    public static Exception ConcurrentUpdateException;
    public static Exception InvalidCommandException;
    public static Exception TableNoIndexException;
    public static Exception InvalidFieldException;
    public static Exception FieldNotFoundException;
    public static Exception InvalidValuesException;
    public static Exception FieldNotIndexedException;
    public static Exception InvalidLogOpException;
    public static Exception DuplicatedTableException;
    public static Exception TableNotFoundException;
    public static Exception NestedTransactionException;
    public static Exception NoTransactionException;
}
