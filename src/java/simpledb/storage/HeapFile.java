package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;

/**
 *  * HeapFile is an implementation of a DbFile that stores a collection of tuples
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
    class HeapFileIterator implements DbFileIterator{
        private int pageNo=0;
        private Iterator<Tuple> iterator=null;
        private HeapPage page=null;
        private int totalPageNumber=0;
        private boolean isOpen=false;

        public HeapFileIterator(){
            try {
                HeapPageId pid=new HeapPageId(getId(), this.pageNo);
                this.page=(HeapPage)Database.getBufferPool().getPage(null, pid, null);
                this.totalPageNumber=(int)(heapFile.length()/Database.getBufferPool().getPageSize());
                this.iterator=this.page.iterator();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.isOpen=true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(isOpen==true) {
                if (iterator.hasNext() == true)
                    return true;
                else {
                    if (this.pageNo == this.totalPageNumber - 1)
                        return false;
                    else {
                        this.pageNo+=1;
                        HeapPageId pid=new HeapPageId(getId(), this.pageNo);
                        this.page=(HeapPage)Database.getBufferPool().getPage(null, pid, null);
                        this.iterator=this.page.iterator();
                        return this.hasNext();
                    }
                }
            }else
                return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(isOpen==true) {
                if (this.hasNext() == false)
                    return null;
                else {
                    if (!this.iterator.hasNext()) {
                        this.pageNo += 1;
                        HeapPageId pid = new HeapPageId(getId(), this.pageNo);
                        this.page = (HeapPage) Database.getBufferPool().getPage(null, pid, null);
                        this.iterator = this.page.iterator();
                    }
                    return this.iterator.next();
                }
            }else
                //若尚未open，则应抛出NoSuchElementException
                throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if(isOpen==true){
                this.pageNo=0;
                try {
                    HeapPageId pid=new HeapPageId(getId(), this.pageNo);
                    this.page=(HeapPage)Database.getBufferPool().getPage(null, pid, null);
                    this.totalPageNumber=(int)(heapFile.length()/Database.getBufferPool().getPageSize());
                    this.iterator=this.page.iterator();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            else
                return;
        }

        @Override
        public void close() {
            this.isOpen=false;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        // File类是一个Java.io中的类，代表着一个文件路径的抽象
        this.heapFile=f;
        this.tupleDesc=td;
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
    public Page readPage(PageId pid) {
        // some code goes here
        long offset=pid.getPageNumber()*Database.getBufferPool().getPageSize();
        byte[] buffer=new byte[Database.getBufferPool().getPageSize()];
        HeapPage result=null;
        File file=this.heapFile;
        try {
            RandomAccessFile raf=new RandomAccessFile(this.heapFile, "r");
            raf.seek(offset);
            int count=0;
            raf.read(buffer);
            HeapPage page=new HeapPage((HeapPageId)pid, buffer);
            result=page;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNo=page.getId().getPageNumber();
        RandomAccessFile raf=new RandomAccessFile(this.heapFile, "rw");
        raf.seek(Database.getBufferPool().getPageSize()*pageNo);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //使用file.length()获取file的长度
        //
        long fileLength=this.heapFile.length();
        return (int)(fileLength/Database.getBufferPool().getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> list=new ArrayList<>();
        int numPages=this.numPages();
        HeapPage heapPage;
        for(int i=0;i<numPages;++i){
            HeapPageId heapPageId=new HeapPageId(getId(), i);
            heapPage=((HeapPage)Database.getBufferPool().getPage(tid, heapPageId, null));
            if(heapPage.getNumEmptySlots()>0){
                heapPage.insertTuple(t);
                list.add(heapPage);
                return list;
            }
        }
        //若均空，则新建page
        HeapPageId heapPageId=new HeapPageId(getId(), this.numPages());
        heapPage=new HeapPage(heapPageId, HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        RandomAccessFile raf=new RandomAccessFile(this.heapFile, "rw");
        raf.seek(this.numPages()*Database.getBufferPool().getPageSize());
        raf.write(heapPage.getPageData());
        raf.close();
        list.add(heapPage);
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> list=new ArrayList<>();
        HeapPage heapPage=(HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), null);
        heapPage.deleteTuple(t);
        list.add(heapPage);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator();
    }

    private File heapFile;
    private TupleDesc tupleDesc;
}

