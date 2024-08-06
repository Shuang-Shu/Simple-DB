package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * * HeapFile is an implementation of a DbFile that stores a collection of
 * tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * HeapFile是DbFile的一个实现，它不按特定顺序存储图元的集合。
 * 图元被存储在页面上，每个页面的大小是固定的，而文件只是这些页面
 * 的集合。HeapFile与HeapPage紧密合作。HeapPages的格式在
 * HeapPage构造函数中描述。
 *
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    class HeapFileIterator implements DbFileIterator {
        private int pageIndex = 0;
        private Iterator<Tuple> iterator = null;
        private HeapPage page = null;
        private int totalPageNumber = 0;
        private boolean open = false;
        private TransactionId transactionId;

        public HeapFileIterator(TransactionId tid) {
            try {
                // 事务的ID
                this.transactionId = tid;
                HeapPageId pid = new HeapPageId(getId(), this.pageIndex);
                this.page = (HeapPage) Database.getBufferPool().getPage(transactionId, pid, Permissions.READ_WRITE);
                Database.getBufferPool();
                this.totalPageNumber = (int) (heapFile.length() / BufferPool.getPageSize());
                this.iterator = this.page.iterator();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.open = true;
        }

        @Override
        public boolean hasNext() throws DbException {
            if (open) {
                if (iterator.hasNext()) {
                    // fast path
                    return true;
                }
                for (int i = pageIndex + 1; i < totalPageNumber; i++) {
                    this.pageIndex = i;
                    var nxtPageId = new HeapPageId(getId(), this.pageIndex);
                    this.page = (HeapPage) Database.getBufferPool().getPage(transactionId, nxtPageId,
                            Permissions.READ_WRITE);
                    this.iterator = this.page.iterator();
                    if (this.iterator.hasNext()) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (open) {
                if (!this.hasNext()) {
                    throw new NoSuchElementException();
                }
                if (!this.iterator.hasNext()) {
                    this.pageIndex += 1;
                    HeapPageId pid = new HeapPageId(getId(), this.pageIndex);
                    this.page = (HeapPage) Database.getBufferPool().getPage(transactionId, pid,
                            Permissions.READ_WRITE);
                    this.iterator = this.page.iterator();
                }
                return this.iterator.next();
            }
            // 若尚未open，则应抛出NoSuchElementException
            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (open == true) {
                this.pageIndex = 0;
                try {
                    HeapPageId pid = new HeapPageId(getId(), this.pageIndex);
                    this.page = (HeapPage) Database.getBufferPool().getPage(transactionId, pid, Permissions.READ_ONLY);
                    Database.getBufferPool();
                    this.totalPageNumber = (int) (heapFile.length() / BufferPool.getPageSize());
                    this.iterator = this.page.iterator();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else
                return;
        }

        @Override
        public void close() {
            this.open = false;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *          the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        // File类是一个Java.io中的类，代表着一个文件路径的抽象
        this.heapFile = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * 根据文件路径散列，进而获得File的UniqueID
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.heapFile.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        Database.getBufferPool();
        // some code goes here
        long offset = pid.getPageNumber() * BufferPool.getPageSize();
        int num = this.numPages();
        if (pid.getPageNumber() >= num)
            throw new IllegalArgumentException();
        Database.getBufferPool();
        byte[] buffer = new byte[BufferPool.getPageSize()];
        HeapPage result = null;
        try (RandomAccessFile raf = new RandomAccessFile(this.heapFile, "r")) {
            raf.seek(offset);
            raf.read(buffer);
            HeapPage page = new HeapPage((HeapPageId) pid, buffer);
            result = page;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNo = page.getId().getPageNumber();
        RandomAccessFile raf = new RandomAccessFile(this.heapFile, "rw");
        Database.getBufferPool();
        raf.seek(BufferPool.getPageSize() * pageNo);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // 使用file.length()获取file的长度
        // 除已在硬盘中的文件外，还需要考虑BufferPool中可能存在的新建的页面，它们在程序正常运行的情况下将在未来被写入磁盘
        //
        long fileLength = this.heapFile.length();
        Database.getBufferPool();
        return (int) (fileLength / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    // 代表事务将指定的元组插入文件。此方法将在受影响的文件页面上获得一个锁，并可能阻塞，直到获得锁为止。
    // 返回被影响的Page的List
    // 该方法只会在
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, TransactionAbortedException, IOException {
        // some code goes here
        // not necessary for lab1
        List<Page> affectedPages = new ArrayList<>();
        int numPages = numPages();
        HeapPage heapPage = null;
        // TODO why bufferPool->HeapFile->bufferPool?
        for (int i = 0; i < numPages; ++i) {
            // 这里保证了一定找到一个存在的page？
            HeapPageId pageId = new HeapPageId(getId(), i);
            // getPage方法需要获取锁
            // 注意，此处的page是磁盘中page在BufferPool中的镜像，而非磁盘中的对象
            heapPage = ((HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE));
            if (heapPage.getNumEmptySlots() > 0) {
                synchronized (heapPage) {
                    // double check lock
                    if (heapPage.getNumEmptySlots() > 0) {
                        heapPage.insertTuple(t);
                    } else {
                        continue;
                    }
                }
                affectedPages.add(heapPage);
                return affectedPages;
            } else {
                // 若该page中没有空的槽位，直接释放这个page上的锁
                // 根据2PL协议，应当在事务提交时才释放锁；此处由于没有操作page，可以提前释放锁以提高并发度
                Database.getBufferPool().unsafeReleasePage(tid, pageId);
            }
        }
        // need an empty page from buffer pool
        /*
         * 并发的可能性：
         * 1. 没有其它事务（线程）新建Page，此时本线程能通过BufferPool直接新建Page并插入tuple
         * 2. 有其它线程新建Page，但Page未满，此时本线程获取的是其它线程修改完成的Page，直接插入
         * 3. 有其它线程新建Page，且Page已满，
         *
         */
        if (affectedPages.size() == 0) {
            synchronized (Database.getBufferPool()) {
                // double check
                HeapPageId heapPageId = new HeapPageId(getId(), numPages); // new tail page id
                HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
                newPage.insertTuple(t);
                affectedPages.add(newPage);
                return affectedPages;
            }
        }
        return affectedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> list = new ArrayList<>();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),
                Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        list.add(heapPage);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private File heapFile;
    private TupleDesc tupleDesc;
}
