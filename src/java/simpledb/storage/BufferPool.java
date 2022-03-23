package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class Pair<T1, T2>{
    private T1 t1;
    private T2 t2;

    public T1 getT1() {
        return t1;
    }

    public void setT1(T1 t1) {
        this.t1 = t1;
    }

    public T2 getT2() {
        return t2;
    }

    public void setT2(T2 t2) {
        this.t2 = t2;
    }
}

// 锁管理器，管理Page层面上的锁
class LockManager{
    // 存放了持有pageId对应Page的锁的transaction 集合以及锁类型
    Map<PageId, Pair<Set<TransactionId>, Permissions>> locks;
    // 存放了pageId对应的等待条件，当某个page锁可能被释放时该条件被激活
    Map<PageId, Condition> conditionMap;
    // 存放了transactionId所涉及的pageId，这和locks记录的关系是相反的，它是为了加快运行速度
    Map<TransactionId, Set<PageId>> transactionPageMap;
    Lock managerLock;

    // 构造函数
    public LockManager(){
        locks=new HashMap<>();
        conditionMap=new HashMap<>();
        transactionPageMap=new HashMap<>();
        managerLock=new ReentrantLock();
    }

    // 检验申请的permissons与pageId当前的锁是否相容
    public boolean isCompatible(Permissions permissions, PageId pageId){
        managerLock.lock();
        try {
            if (!locks.containsKey(pageId)) return true;
            if (permissions == Permissions.READ_ONLY)
                if (locks.get(pageId).getT2() == Permissions.READ_ONLY) return true;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            managerLock.unlock();
        }
        return false;
    }

    // 尝试获取锁，若成功则返回；否则阻塞
    public void requestLock(TransactionId transactionId, PageId pageId, Permissions permissions){
        managerLock.lock();
        try {
            if (!locks.containsKey(pageId)) {
                // 该page的锁未被获取，设置其被获取，同时新增一个等待条件（起到了事务请求队列的作用）
                Pair<Set<TransactionId>, Permissions> pair = new Pair<>();
                Set<TransactionId> transactionSet = new HashSet<>();
                transactionSet.add(transactionId);
                pair.setT1(transactionSet);
                pair.setT2(permissions);
                locks.put(pageId, pair);
                // 新增等待条件
                conditionMap.put(pageId, managerLock.newCondition());
                // 将transactionId对应的pageId插入transactionPageMap
                if(!transactionPageMap.containsKey(transactionId)){
                    Set<PageId> pageIdSet=new HashSet<>();
                    pageIdSet.add(pageId);
                    transactionPageMap.put(transactionId, pageIdSet);
                }else
                    transactionPageMap.get(transactionId).add(pageId);
                return;
            } else if(!locks.get(pageId).getT1().contains(transactionId)){
                // 该page已上锁，且当前的transactionId不在已获得锁的集合中
                // 不相容的情况，进入阻塞状态
                while (!isCompatible(permissions, pageId)) {
                    try {
                        // 该方法会抛出java.lang.IllegalMonitorStateException异常，为何（await()方法需要被其对应的锁对象的lock()与unlock()方法包裹
                        conditionMap.get(pageId).await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                // 若相容，则将该事务放入拥有page锁的集合中
                if (locks.containsKey(pageId)) {
                    // 此时集合不为空，直接放入即可，不需要处理事务
                    locks.get(pageId).getT1().add(transactionId);
                    // 将transactionId对应的pageId插入transactionPageMap
                    if(!transactionPageMap.containsKey(transactionId)){
                        Set<PageId> pageIdSet=new HashSet<>();
                        pageIdSet.add(pageId);
                        transactionPageMap.put(transactionId, pageIdSet);
                    }else
                        transactionPageMap.get(transactionId).add(pageId);
                } else {
                    // 此时拥有pageId对应的page的锁的事务集合为空，pageId已经从locks和conditionMap中删除，需要重新获取锁并设置等待条件
                    requestLock(transactionId, pageId, permissions);
                }
            }else {
                // 前的transactionId在已获得锁的集合中
                // do nothing
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            managerLock.unlock();
        }
    }

    // 释放锁，释放事务在Page上的锁
    public void releaseLock(TransactionId transactionId, PageId pageId){
        managerLock.lock();
        try {
            if (!locks.containsKey(pageId)) return;
            locks.get(pageId).getT1().remove(transactionId);
            Condition temp = conditionMap.get(pageId);
            if (locks.get(pageId).getT1().isEmpty()) {
                // 此时拥有pageId对应的page的锁的事务集合为空，将其从locks以及conditionMap中删除
                locks.remove(pageId);
                conditionMap.remove(pageId);
            }
            transactionPageMap.get(transactionId).remove(pageId);
            if (transactionPageMap.get(transactionId).isEmpty())
                transactionPageMap.remove(transactionId);
            // 通知所有在该pageId上等待的事务进行尝试
            temp.signalAll();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            managerLock.unlock();
        }
    }

    /*升级事务在Page上的锁，成功则返回true，否则返回false
     升级的规则是:
        如果该page未被上锁，则将其升级为共享锁
        如果该page有共享锁，检查拥有pageId对应的page的锁的事务集合中是否有且仅有transactionId对应的事务，有则升级，否则返回false
     */
    public boolean upgradeLock(TransactionId transactionId, PageId pageId){
        managerLock.lock();
        try {
            if (!locks.containsKey(pageId)) {
                requestLock(transactionId, pageId, Permissions.READ_ONLY);
                return true;
            }
            if (locks.get(pageId).getT2() == Permissions.READ_WRITE)
                return false;
            if (locks.get(pageId).getT1().size() == 1 && locks.get(pageId).getT1().contains(transactionId)) {
                locks.get(pageId).setT2(Permissions.READ_WRITE);
                return true;
            } else
                return false;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            managerLock.unlock();
        }
        return false;
    }

    // 查看事务transactionId是否持有pageId上的锁
    public Permissions peekPermisson(TransactionId transactionId, PageId pageId){
        managerLock.lock();
        try {
            if (locks.containsKey(pageId))
                if (locks.get(pageId).getT1().contains(transactionId))
                    return locks.get(pageId).getT2();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            managerLock.unlock();
        }
        return null;
    }
}

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    //每个page的默认大小为4096Bytes
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private static LockManager lockManager=new LockManager();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.bufferPool=new ArrayList<>(numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        //若未在bufferPool中找到对应的pid
            //若bufferPool未满
        try {
            // 要考虑读取的权限问题
            //在bufferPool中查找对应的pid
            lockManager.requestLock(tid, pid, perm);
            Iterator<Page> iterator=this.bufferPool.iterator();
            while(iterator.hasNext()){
                Page temp=iterator.next();
                if(temp!=null&&temp.getId().equals(pid))
                    return temp;
            }
            if (this.bufferPool.size() < this.DEFAULT_PAGES) {
                //catalog单例中记录了数据库的全部信息，通过pid可以获取表的信息
                try {
                    Page temp = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    this.bufferPool.add(temp);
                    return temp;
                } catch (NoSuchElementException e) {
                    return null;
                }
            } else {
                //若bufferPool已满，目前先抛出DbException
                this.evictPage();
                return this.getPage(tid, pid, perm);
            }
        }finally {
            lockManager.releaseLock(tid, pid);
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        Set<PageId> set=lockManager.transactionPageMap.get(tid);
        for(PageId pageId:set)
            lockManager.releaseLock(tid, pageId);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        if(lockManager.peekPermisson(tid, pid)!=null)
            return true;
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile=Database.getCatalog().getDatabaseFile(tableId);
        List<Page> list=new ArrayList<>();
        list=dbFile.insertTuple(tid, t);
        for(Page page:list) {
            page.markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        try {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
            List<Page> list;
            list=dbFile.deleteTuple(tid, t);
            for(Page page:list) {
                page.markDirty(true, tid);
            }
        }catch (NoSuchElementException e){
            //若未找到，则不进行删除
            return;
        }
    }

    /**
     * 将所有脏页刷新到磁盘。
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(int i=0;i<this.bufferPool.size();++i){
            if(this.bufferPool.get(i)!=null&&this.bufferPool.get(i).isDirty()!=null)
                this.flushPage(this.bufferPool.get(i).getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        int idx=-1;
        for(int i=0;i<this.bufferPool.size();++i){
            if(this.bufferPool.get(i).getId().equals(pid)) {
                idx = i;
                break;
            }
        }
        if(idx==-1)
            return;
        else{
            this.bufferPool.set(idx, this.bufferPool.get(this.bufferPool.size()-1));
            this.bufferPool.remove(this.bufferPool.size()-1);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Iterator<Page> iterator=this.bufferPool.iterator();
        while (iterator.hasNext()){
            Page temp=iterator.next();
            if(temp.getId().equals(pid)){
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(temp);
                return;
            }
        }
        return;
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //仅仅简单的替换掉第一个page
        try {
            this.flushPage(this.bufferPool.get(0).getId());
            this.discardPage(this.bufferPool.get(0).getId());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private ArrayList<Page> bufferPool=null;
}
