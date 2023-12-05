package cn.yu.db.backend.common;

import cn.yu.db.common.DbException;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Yu
 * @description 引用计数策略缓存
 * @date 2023-08-07
 */

public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while (true){
            lock.lock();
            if (getting.containsKey(key)) {
                lock.unlock();
                Thread.sleep(1);
                continue;
            }
            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key)+1);
                lock.unlock();
                return obj;
            }
            //没有在缓存中，获取该资源
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw DbException.CacheFullException;
            }
            count ++ ;
            getting.put(key, true);
            lock.unlock();
            break;
        }
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        return obj;
    }

    //释放缓存
    protected void release(long key){
        lock.lock();
        int ref = references.get(key) - 1;
        if(ref == 0){
            T obj = cache.get(key);
            releaseForCache(obj);
            references.remove(key);
            cache.remove(key);
            count--;
        }else{
            references.put(key, ref);
        }
        lock.unlock();
    }

    protected void close(){
        lock.lock();
        Set<Long> keys = cache.keySet();
        for (Long key : keys) {
            T obj = cache.get(key);
            releaseForCache(obj);
            references.remove(key);
            cache.remove(key);
        }
        lock.unlock();
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
