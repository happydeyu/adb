package cn.yu.db.backend.im;

import cn.yu.db.backend.common.SubArray;
import cn.yu.db.backend.dm.dataitem.DataItem;
import cn.yu.db.backend.dm.manager.DataManager;
import cn.yu.db.backend.tm.TransactionManagerImpl;
import cn.yu.db.utils.ByteUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-17
 */

public class BPlusTree {

    DataManager dm;
    long bootUid;
    DataItem bootDataItem;
    Lock bootLock;

    //返回数根的uid
    public static long create(DataManager dm) throws Exception{
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        return dm.insert(TransactionManagerImpl.SUPER_XID, ByteUtil.longToByte(rootUid));
    }
    public static BPlusTree load(long bootUid, DataManager dm) throws Exception{
        DataItem bootDataItem = dm.read(bootUid);
        BPlusTree t = new BPlusTree();
        t.bootUid = bootUid;
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long searchLeaf(long nodeUid, long key) throws Exception {
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    private long searchNext(long nodeUid, long key) throws Exception {
        while(true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception {
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception {
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        List<Long> uids = new ArrayList<>();
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            Node.LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid == 0) {
                break;
            } else {
                leafUid = res.siblingUid;
            }
        }
        return uids;
    }

    public void insert(long key, long uid) throws Exception{
        long rootUid = rootUid();
        InsertRes res = insert(rootUid, uid, key);
        if (res.newNode!=0) {
            updateRootUid(rootUid, uid, key);
        }
    }
    private void updateRootUid(long left, long right, long rightKey) throws Exception {
        bootLock.lock();
        try {
            byte[] rootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rootRaw);
            bootDataItem.before();
            SubArray diRaw = bootDataItem.data();
            System.arraycopy(ByteUtil.longToByte(newRootUid), 0, diRaw.raw, diRaw.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception{
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        InsertRes res = null;
        if (isLeaf) {
            res = insertAndSplit(nodeUid, uid, key);
        }

    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception {

        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            Node.InsertAndSplitRes iasr = node.insertAndSplit(uid, key);
            node.release();
            if (iasr.siblingUid!=0) {
                nodeUid = iasr.siblingUid;
            }else {
                InsertRes res = new InsertRes();
                res.newNode = iasr.newSon;
                res.newKey = iasr.newKey;
                return res;
            }
        }
    }
    public void close(){
        bootDataItem.release();
    }

    class InsertRes{
        long newNode, newKey;
    }

    private long rootUid(){
        bootLock.lock();
        try {
            SubArray sa = bootDataItem.data();
            return ByteUtil.byteToLong(Arrays.copyOfRange(sa.raw, sa.start, sa.start+8));
        } finally {
            bootLock.unlock();
        }
    }

}
