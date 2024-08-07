package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
class ByteTool {
    public static byte[] valAtI = { 1, 2, 4, 8, 16, 32, 64, -128 };

    public static boolean isSet(byte b, int i) {
        if ((b & valAtI[i]) != 0)
            return true;
        else
            return false;
    }

    public static int oneNum(byte b) {
        int result = 0;
        while (b != 0) {
            b = (byte) (b & (b - 1));
            result += 1;
        }
        return result;
    }

    public static byte clear(byte b, int i) {
        b = (byte) (b & (~(valAtI[i])));
        return b;
    }

    public static byte set(byte b, int i) {
        b = (byte) (b | (valAtI[i]));
        return b;
    }
}

public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    private boolean dirty;
    TransactionId tid;

    byte[] oldData;
    @SuppressWarnings("removal")
    private final Byte oldDataLock = new Byte((byte) 0); // use new to ensure a different object

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to:
     * <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p>
     * where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     * 
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        this.dirty = false;
        this.tid = null;

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     * 
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here
        return (int) (8 * BufferPool.getPageSize()
                / (8 * Database.getCatalog().getTupleDesc(this.pid.getTableId()).getSize() + 1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each
     * tuple occupying tupleSize bytes
     * 
     * @return the number of bytes in the header of a page in a HeapFile with each
     *         tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        return (int) (Math.ceil((double) numSlots / 8));
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            // should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); // - numSlots *
                                                                                                 // td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; // all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should
     * be updated to reflect
     * that it is no longer stored on any page.
     * 
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab
        // 注意，tuple本身的recodrId已经标识了它的位置
        int i = t.getRecordId().getTupleNumber();
        if (this.isSlotUsed(i) && this.pid == t.getRecordId().getPageId()) {
            this.tuples[i] = null;
            int base = i / 8;
            byte b = this.header[base];
            int offset = i % 8;
            this.header[base] = ByteTool.clear(b, offset); // lazy delete
            return;
        }
        throw new DbException("page with pid: " + t.getRecordId().getPageId() + " not found");
    }

    /**
     * Adds the specified tuple to the page; the tuple should be updated to reflect
     * that it is now stored on this page.
     * 
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        // 注意，要记得更新元组的相关信息
        // 首先在tuples中寻找空位
        for (int i = 0; i < this.tuples.length; ++i) {
            if (this.tuples[i] == null) {
                RecordId recordId = new RecordId(pid, i);
                t.setRecordId(recordId);
                int base = i / 8;
                byte b = this.header[base];
                int offset = i % 8;
                this.header[base] = ByteTool.set(b, offset);
                this.tuples[i] = t;
                return;
            }
        }
        throw new DbException("no empty slot in page: " + this.pid);
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction that did the
     * dirtying
     * 将 此页面标记为脏/不脏，并记录进行脏操作的事务
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        this.dirty = dirty;
        this.tid = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if
     * the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        if (this.dirty == true)
            return this.tid;
        else
            return null;
    }

    /**
     * Returns the number of empty slots on this page.
     * 返回该page中空槽位的数量
     * 即：根据header中各比特位的状态确定可用槽位数量
     */
    public int getNumEmptySlots() {
        // some code goes here
        // TODO: 1-bit count algorithm can be optimized
        int byteNum = numSlots / 8;
        int result = 0;
        for (int i = 0; i < byteNum; ++i)
            result += ByteTool.oneNum(header[i]);
        int rest = numSlots - 8 * byteNum;
        for (int i = 0; i < rest; ++i) {
            if (ByteTool.isSet(header[byteNum], i))
                result += 1;
        }
        return numSlots - result;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        int byteIndex = i / 8;
        int offset = i % 8;
        return ByteTool.isSet(header[byteIndex], offset);
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        // 不必要，已经被集成
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this
     *         iterator throws an UnsupportedOperationException)
     *         (note that this iterator shouldn't return tuples in empty slots!)
     */
    class MyIterator implements Iterator<Tuple> {
        /**
         * two stats:
         * * tuples[currentIndex]==null: no record in this page, haxNext() returns false
         * * tuples[currentIndex]!=null: record in this page, hasNext() returns true
         */
        private int currentIndex = 0;

        public boolean hasNext() {
            if (tuples == null || tuples.length == 0 || currentIndex >= tuples.length) {
                return false;
            }
            return tuples[currentIndex] != null;
        }

        public Tuple next() {
            // TODO: empty slot is not checked
            currentIndex++;
            return tuples[currentIndex - 1];
        }
    }

    public Iterator<Tuple> iterator() {
        // some code goes here
        return new MyIterator();
    }

}