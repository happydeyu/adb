package cn.yu.db.backend.dm.pageindex;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-11
 */

public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
