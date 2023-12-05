package cn.yu.db.backend.vm;

import cn.yu.db.backend.tm.TransactionManager;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-15
 */

public class Visibility {

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if (t.level==0) {
            return false;
        } else {
            return tm.isCommitted(t.xid) && (xmax>t.xid || t.isInSnapshot(xmax));
        }

    }

    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }
}
