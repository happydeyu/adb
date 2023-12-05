package cn.yu.db.backend.vm;

import cn.yu.db.common.DbException;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yu
 * @description 维护依赖等待图，检测死锁
 * @date 2023-08-12
 */

public class LockTable {
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    public Lock add(long xid, long uid) throws Exception{
        lock.lock();
        try {


            if (isInList(x2u, xid, uid)) {  //已经持有，不需要等待
                return null;
            }
            if (!u2x.containsKey(uid)) {    //uid没有被某个xid持有，不需要等待
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            //剩余以下需要等待的情况
            waitU.put(xid, uid);    //xid等待uid
            putIntoList(wait, uid, xid);
            if (hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw DbException.DeadLockException;
            }
            ReentrantLock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        }finally {

            lock.unlock();
        }
    }

    private void removeFromList(Map<Long, List<Long>> map, long id0, long id1) {
        List<Long> list = map.get(id0);
        if (list==null) {
            return ;
        }
        Iterator<Long> i = list.iterator();
        while (i.hasNext()) {
            Long e = i.next();
            if (e==id1) {
                i.remove();
                break;
            }
        }
        if (list.size()==0) {
            map.remove(id0);
        }
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    /*
    x2u和u2x维护了一个图
    x2u表示x拥有u的列表(1对多)，u2x表示u被某个x持有
    思想：在等待图中，根据xid找uid，在根据uid持有的xid1...，如果有环，说明产生了死锁
     */
    private boolean hasDeadLock(){
        xidStamp = new HashMap<>();
        stamp = 1;
        for (Long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if (s != null && s>0) {   //访问过
                continue;
            }
            stamp ++;
            if (dfs(xid)){
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid){
        Integer s = xidStamp.get(xid);
        if (s!=null && s==stamp) {
            return true;    //同一连通图，有环
        }
        if (s!=null && s<stamp){
            return false;   //之前遍历过
        }
        //没遍历过
        xidStamp.put(xid, stamp);
        Long uid = waitU.get(xid);
        if (uid == null) {
            return false;   //该xid没有等待uid
        }
        Long x = u2x.get(uid);//找出持有该uid的xid，继续递归
        return dfs(x);
    }

    private void putIntoList(Map<Long, List<Long>> map, long id0, long id1){
        if (!map.containsKey(id0)) {
            map.put(id0, new ArrayList<>());
        }
        map.get(id0).add(0, id1);   //插入到最前边
    }

    private boolean isInList(Map<Long, List<Long>> map, long id0, long id1){
        List<Long> list = map.get(id0);
        if (list == null) {
            return false;
        }
        Iterator<Long> iterator = list.iterator();
        while (iterator.hasNext()){
            Long e = iterator.next();
            if (e == id1) {
                return true;
            }
        }
        return false;
    }
    /*
        事务commit或abort时，释放持有的锁，从图中移除
     */
    public void remove(long xid){
        lock.lock();
        List<Long> l = x2u.get(xid);
        if (l != null) {
            while (l.size() > 0) {
                Long uid = l.remove(0);
                selectNewXid(uid);
            }
        }
        waitU.remove(xid);
        x2u.remove(xid);
        waitLock.remove(xid);
        lock.unlock();

    }
    private void selectNewXid(long uid){
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if (l==null) {
            return;
        }
        while (l.size() > 0) {
            Long xid = l.remove(0);
            if (!waitLock.containsKey(xid)) {
                //不需要锁
                continue;
            }else {
                //有锁
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }

        }
        if (l.size()==0) {
            wait.remove(uid);
        }
    }
}
