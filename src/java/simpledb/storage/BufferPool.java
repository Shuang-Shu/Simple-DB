package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.beans.Transient;
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

// 一个集合邻接表实现的图对象
/*
@Date      :
---------
    2022/04/21 11:00:58
Description:
---------
    邻接表的图实现，支持有向/无向图
    实现中列表被Map代替以提高效率
*/
class AdjacencyListGraph<T, V> {
    Map<T, Set<EdgeNode<T, V>>> vertexMap=new HashMap<>();
    /*
    @Description  :
    ---------
        向图中插入一条边，如已有边<start, end>，则覆盖其value
    @Param        :
    ---------
        T start
            起点的id
        T end
            终点的id
    @Returns      :
    ---------
        void
    */
    public void insertEdge(T start, T end, V value){
        EdgeNode<T, V> newEdge=new EdgeNode<>(end, value);
        if(end==null||start==null)
            throw new NullPointerException("start与end不能为null");
        if(!vertexMap.containsKey(start)){
            Set<EdgeNode<T, V> > newSet=new HashSet<>();
            newSet.add(newEdge);
            vertexMap.put(start, newSet);
        }else{
            Set<EdgeNode<T, V> > oldSet=vertexMap.get(start);
            if(!oldSet.contains(newEdge)){
                // 不包含该边，加入
            }else{
                // 包含该边，修改值
                oldSet.remove(newEdge);
            }
            oldSet.add(newEdge);
        }
    }
    /*
    @Description  :
    ---------
        判断图中是否有某条边
    @Param        :
    ---------

    @Returns      :
    ---------
        true
            含有
        false
            不含有
    */
    public boolean containsEdge(T start, T end){
        EdgeNode<T, V> edge=new EdgeNode<>(end, null);
        if(!vertexMap.containsKey(start))
            return false;
        else{
            if(!vertexMap.get(start).contains(edge))
                return false;
        }
        return true;
    }

    /*
    @Description  :
    ---------
        删除图中的边
    @Param        :
    ---------

    @Returns      :
    ---------
        true
            删除成功
        false
            删除失败
    */
    public boolean removeEdge(T start, T end){
        if(!vertexMap.containsKey(start)){
            return false;
        }else{
            Set<EdgeNode<T, V>> temp=vertexMap.get(start);
            EdgeNode<T, V> targetEdge=new EdgeNode<>(end, null);
            if(!temp.contains(targetEdge))
                return false;
            else{
                temp.remove(targetEdge);
                if(temp.isEmpty())
                    vertexMap.remove(start);
            }
        }
        return true;
    }

    /*
    @Description  :
    ---------
        基于DFS的环检测算法，检测是否有经过originStart的环
    @Param        :
    ---------
        T originStart
            环探测的开始位置
        T start
            DFS的开始位置
        Set<T> path
            记录已遍历的节点，由调用者提供
    @Returns      :
    ---------
        true
            存在环
        false
            不存在环
    */
    public boolean circleDetect(T originStart, T start, Set<T> path){
        Set<EdgeNode<T, V>> next=vertexMap.get(start);
        if(next!=null){
            for(EdgeNode<T, V> edge: next){
                if(edge.getId().equals(originStart))
                    return true;
                if(!path.contains(edge.getId())){
                    path.add(edge.getId());
                    return circleDetect(originStart, edge.getId(), path);
                }
            }
        }
        return false;
    }

    /*
    @Description  :
    ---------
        删去图中所有与point相关的边，包括入边和出边
    @Param        :
    ---------

    @Returns      :
    ---------

    */
    public void removeVertex(T vertex){
        vertexMap.remove(vertex);
        EdgeNode<T, V> tempEdge=new EdgeNode<>(vertex, null);
        for(T element:vertexMap.keySet()){
            if(vertexMap.get(element).contains(tempEdge)){
                vertexMap.get(element).remove(tempEdge);
            }
            if(vertexMap.get(element).isEmpty())
                vertexMap.remove(element);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        for(T vertex:vertexMap.keySet()){
            sb.append(vertex.toString());
            sb.append("->");
            Set<EdgeNode<T, V> > temp=vertexMap.get(vertex);
            for(EdgeNode<T, V> edge:temp){
                sb.append("("+edge.getId().toString()+","+edge.getValue().toString()+")");
                sb.append("->");
            }
            sb=new StringBuilder(sb.substring(0, sb.length()-2));
            sb.append("\n");
        }
        return sb.toString();
    }
}

/*
@Date      :
---------
    2022/04/21 11:02:29
Description:
---------
    边节点，重写了hashCode和equals方法
    此处两边相等仅考虑id，即目标边是否相同，注意，EdgeNode只在vertexMap中存在意义
*/
class EdgeNode<T, V>{
    T id;// 目标节点的ID
    V value;// 边的值

    public EdgeNode(T targetId, V value){
        id=targetId;
        this.value=value;
    }

    public T getId() {
        return this.id;
    }

    public void setId(T id) {
        this.id = id;
    }

    public V getValue() {
        return this.value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return id.hashCode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        if(obj==null)
            return false;
        if(obj.hashCode()!=hashCode())
            return false;
        if(this==obj)
            return true;
        if(!(obj instanceof EdgeNode))
            return false;
        EdgeNode<T, V> temp=(EdgeNode<T, V>)obj;
        if(id.equals(temp.getId()))
            return true;
        else
            return false;
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
    // 用于对LockManager加锁
    Lock managerLock;
    // 用于进行死锁检测的等待图
    AdjacencyListGraph<TransactionId, Integer> waitGraph;

    // 构造函数
    public LockManager(){
        locks=new HashMap<>();
        conditionMap=new HashMap<>();
        transactionPageMap=new HashMap<>();
        managerLock=new ReentrantLock();
        waitGraph=new AdjacencyListGraph<>();
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
                        // 在等待图中新建边
                        Set<TransactionId> targetTidSet=locks.get(pageId).getT1();
                        for(TransactionId tid:targetTidSet)
                            waitGraph.insertEdge(transactionId, tid, null);
                        // 检查环的存在
                        if(waitGraph.circleDetect(transactionId, transactionId, new HashSet<>()))
                            throw new TransactionAbortedException();

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
    public static final int DEFAULT_PAGES = 450;
    private LockManager lockManager=new LockManager();
    // 这个锁用于保证只有一个事务能够新建进程
    Lock newPageLock=new ReentrantLock();

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
            throws DbException {
        // some code goes here
        //若未在bufferPool中找到对应的pid
            //若bufferPool未满
        try {
            // 要考虑读取的权限问题
            // 在bufferPool中查找对应的pid
            // 请求加锁
            lockManager.requestLock(tid, pid, perm);
            Iterator<Page> iterator=this.bufferPool.iterator();
            while(iterator.hasNext()){
                // 检查当前bufferpool中是否有所需的page
                Page temp=iterator.next();
                if(temp!=null&&temp.getId().equals(pid))
                    return temp;
            }
            if (this.bufferPool.size() < DEFAULT_PAGES) {
                // catalog单例中记录了数据库的全部信息，通过pid可以获取表的信息
                // 若bufferpool有剩余，则直接在bufferpool中创建一个page镜像
                try {
                    Page temp = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    this.bufferPool.add(temp);
                    // 由于此时temp被返回，如果权限为读/写，则应将其标记为dirty
                    if(perm==Permissions.READ_WRITE)
                        temp.markDirty(true, tid);
                    return temp;
                } catch (IllegalArgumentException e) {
                    // 此时磁盘文件中无此page
                    // 此时应该直接在BufferPool中新建一个Page
                    // 注意！这里与HeapPage类型的文件耦合
                    // 需要获取一个空内容的HeapPage
                    int len=BufferPool.getPageSize();
                    byte[] blankData=new byte[len];
                    Page temp= null;
                    try {
                        temp = new HeapPage((HeapPageId) pid, blankData);
                        this.bufferPool.add(temp);
                        // 此处将新创建的页面直接写回磁盘
                        this.flushPage(pid);
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                    return temp;
                }
            } else {
                //若bufferPool已满，目前先抛出DbException
                // 已添加页面置换，简单地替换掉第一个页面
                this.evictPage();
                return this.getPage(tid, pid, perm);
            }
        }finally {
            // 不应该在某个操作中释放锁，锁的释放应由事务控制
            // lockManager.releaseLock(tid, pid);
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
        try {
            transactionComplete(tid, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
        if(commit){
            // 成功commit的情况
            // 持久化BufferPool中所有与tid相关的page
            for(int i=0;i<bufferPool.size();++i){
                TransactionId temp=bufferPool.get(i).isDirty();
                if(temp==tid){
                    // 该page应该被持久化
                    try {
                        Page flushPage=bufferPool.get(i);
                        // ---------- Lab6添加的内容
                        flushPage.setBeforeImage();
                        // ----------
                        flushPage(flushPage.getId());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }else {
            // 事务异常终止的情况
            for(int i=0;i<bufferPool.size();++i){
                TransactionId temp=bufferPool.get(i).isDirty();
                if(temp==tid){
                    // 该page应该被恢复为磁盘中的状态
                    int tableId=bufferPool.get(i).getId().getTableId();
                    PageId pid=bufferPool.get(i).getId();
                    bufferPool.set(i, Database.getCatalog().getDatabaseFile(tableId).readPage(pid));
                }
            }
        }
        // 在等待图中移除所有与事务相关的边
        lockManager.waitGraph.removeVertex(tid);
        // 释放所有与该tid相关的锁
        Set<PageId> set=lockManager.transactionPageMap.get(tid);
        Set<PageId> clonedSet=new HashSet<>();
        for(PageId pageId:set)
            clonedSet.add(pageId);
        for(PageId pageId:clonedSet)
            lockManager.releaseLock(tid, pageId);
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
     * 代表事务tid向指定的表添加一个元组。将在元组被添加到的页面和任何其他被更新的页面上获取写锁(lab2不需要获取锁)。如果无法获得锁，可能会阻塞。通过调用这些页面的markDirty位，将所有被该操作弄脏的页面标记为dirty，并将所有被弄脏的页面的版本添加到缓存中(替换那些页面的任何现有版本)，以便将来的请求看到最新的页面。
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    // 注意，此处需要将所有被污染的Page插入BufferPool
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile=Database.getCatalog().getDatabaseFile(tableId);
        List<Page> list=dbFile.insertTuple(tid, t);
        // 这里的处理耦合了HeapFile，若使用其他类型的DbFile可能会出错
//        int pageNum=((HeapFile)dbFile).numPages();
        boolean contains;
        for(Page page:list) {
            page.markDirty(true, tid);
            // 将DirtyPage写入BufferPool
            contains=false;
            for(Page cachedPage:bufferPool){
                if(cachedPage.getId()==page.getId()){
                    cachedPage=page;
                    contains=true;
                }
            }
            if(contains==false)
                bufferPool.add(page);
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
        //
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
     * NB: Be careful using this routine -- it writes dirty data to disk so will break simpledb if running in NO STEAL mode.
     *
     *
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
        // 这个操作只能在bufferpool已满的情况下进行，因为它调换的是最后一个slot中的page与pid对应的page
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
                // ---------- Lab6 添加的内容
                TransactionId dirtier = temp.isDirty();
                if (dirtier != null){
                    Database.getLogFile().logWrite(dirtier, temp.getBeforeImage(), temp);
                    Database.getLogFile().force();
                }
                // ----------
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
     * 从缓冲池中丢弃一个页面。将该页刷新到磁盘，以确保更新磁盘上的脏页
     * 此时写入磁盘的数据会覆盖满足一致性的数据，此时原始数据应该被记录，以防止事务被中断
     * @throws DbException
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // 仅仅简单的替换掉第一个page
        // Lab4中需要修改该方法，保证不置换一个DirtyPage
        // 通过简单的遍历实现
        int index=-1;
        for(int i=0;i<bufferPool.size();++i){
            if(bufferPool.get(i).isDirty()==null){
                index=i;
                break;
            }
        }
        if(index!=-1){
            try {
                this.flushPage(this.bufferPool.get(index).getId());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            this.discardPage(this.bufferPool.get(index).getId());
        }else{
            throw new DbException("BufferPool中全为脏页，无法置换");
        }
    }
    // 自己添加的函数，用于rollback
    public void setPage(TransactionId tid, PageId pid, Permissions perm, Page oldPage, Page newPage){
        int index=-1;
        int count=0;
        for(Page page:bufferPool){
            if(page.getId().equals(pid)){
                index=count;
                break;
            }
            count++;
        }
        if(index==-1){
            try {
                count=0;
                getPage(tid, pid, Permissions.READ_WRITE);
                for(Page page:bufferPool){
                    if(page.getId().equals(pid)){
                        index=count;
                        break;
                    }
                    count++;
                }
            } catch (DbException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        // lockManager.requestLock(tid, bufferPool.get(index).getId(), perm);
        bufferPool.set(index, newPage);
    }

    private ArrayList<Page> bufferPool=null;
}
