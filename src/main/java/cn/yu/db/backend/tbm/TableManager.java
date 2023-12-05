package cn.yu.db.backend.tbm;

import cn.yu.db.backend.dm.manager.DataManager;
import cn.yu.db.backend.parser.statement.*;
import cn.yu.db.backend.vm.VersionManager;
import cn.yu.db.utils.ByteUtil;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-28
 */

public interface TableManager {
    BeginRes begin(Begin begin);
    byte[] commit(long xid) throws Exception;
    byte[] abort(long xid);

    byte[] show(long xid);
    byte[] create(long xid, Create create) throws Exception;

    byte[] insert(long xid, Insert insert) throws Exception;
    byte[] read(long xid, Select select) throws Exception;
    byte[] update(long xid, Update update) throws Exception;
    byte[] delete(long xid, Delete delete) throws Exception;

    public static TableManager create(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.create(path);
        booter.update(ByteUtil.longToByte(0));
        return new TableManagerImpl(vm, dm, booter);
    }

    public static TableManager open(String path, VersionManager vm, DataManager dm) {
        Booter booter = Booter.open(path);
        return new TableManagerImpl(vm, dm, booter);
    }
}
