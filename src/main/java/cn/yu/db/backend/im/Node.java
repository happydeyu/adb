package cn.yu.db.backend.im;

import cn.yu.db.backend.common.SubArray;
import cn.yu.db.backend.dm.dataitem.DataItem;
import cn.yu.db.backend.tm.TransactionManagerImpl;
import cn.yu.db.utils.ByteUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Yu
 * @description TODO
 * @date 2023-08-17
 */

public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    public static byte[] newNilRootRaw() {

        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);
        return raw.raw;
    }

    private static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(ByteUtil.longToByte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    private static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(ByteUtil.shortToByte((short) noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    private static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if (isLeaf){
            raw.raw[raw.start+IS_LEAF_OFFSET] = (byte)1;
        }else{
            raw.raw[raw.start+IS_LEAF_OFFSET] = (byte)0;

        }
    }


    public static Node loadNode(BPlusTree bTree, long uid) throws Exception {
        DataItem di = bTree.dm.read(uid);
        Node node = new Node();
        node.tree = bTree;
        node.dataItem = di;
        node.raw = di.data();
        node.uid = uid;
        return node;

    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIsLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    private boolean getRawIsLeaf(SubArray raw) {

        return raw.raw[raw.start+IS_LEAF_OFFSET] == (byte) 1;
    }

    public void release() {
        dataItem.release();
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception{
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();
        dataItem.before();
        try {
            success = insert(uid, key);
            if (!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if (needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch (Exception e) {
                    err = e;
                    throw e;
                }
            }else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }
    class SplitRes {
        long newSon, newKey;
    }
    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }
    private boolean insert(long uid, long key){
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while (kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if (ik < key) {
                kth++;
            } else {
                break;
            }
        }
        if(kth == noKeys && getRawSibling(raw) !=0) return false;
        if(getRawIsLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys+1);
        } else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);
        }
        return true;

    }


    static long getRawSibling(SubArray raw) {
        return ByteUtil.byteToLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }
    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(ByteUtil.longToByte(uid), 0, raw.raw, offset, 8);
    }
    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return ByteUtil.byteToLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }
    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(ByteUtil.longToByte(key), 0, raw.raw, offset, 8);
    }
    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }
    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }

    static byte[] newRootRaw(long left, long right, long key)  {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false);
        setRawNoKeys(raw, 2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, key, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }




    class SearchNextRes {
        long uid;
        long siblingUid;
    }

    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try {
            SearchNextRes res = new SearchNextRes();
            int noKeys = getRawNoKeys(raw);
            for(int i = 0; i < noKeys; i ++) {
                long ik = getRawKthKey(raw, i);
                if(key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;

        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        try {
            int noKeys = getRawNoKeys(raw);
            int kth = 0;
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) {
                    break;
                }
                kth ++;
            }
            List<Long> uids = new ArrayList<>();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    uids.add(getRawKthSon(raw, kth));
                    kth ++;
                } else {
                    break;
                }
            }
            long siblingUid = 0;
            if(kth == noKeys) {
                siblingUid = getRawSibling(raw);
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;
            res.siblingUid = siblingUid;
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }
    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIsLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIsLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }



    public static long getRawKthKey(SubArray raw, int kth) {

        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return ByteUtil.byteToLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    private int getRawNoKeys(SubArray raw) {
        return ByteUtil.byteToShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));

    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

}
