package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *                The transaction running the insert.
     * @param child
     *                The child operator from which to read tuples to be inserted.
     * @param tableId
     *                The table in which to insert tuples.
     * @throws DbException
     *                     if TupleDesc of child differs from table into which we
     *                     are to
     *                     insert.
     */
    private OpIterator child;
    private TransactionId t;
    private int tableId;
    private Tuple result;

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.child = child;
        this.t = t;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] types = { Type.INT_TYPE };
        TupleDesc td = new TupleDesc(types);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.result = null;
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // insert is not executed
        if (result != null) {
            return null;
        }
        Tuple nextTuple = null;
        Type[] types = { Type.INT_TYPE };
        TupleDesc td = new TupleDesc(types);
        result = new Tuple(td);
        int count = 0;
        try {
            while (this.child.hasNext()) {
                nextTuple = this.child.next();
                Database.getBufferPool().insertTuple(t, this.tableId, nextTuple);
                count += 1;
            }
            result.setField(0, new IntField(count));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] children = { this.child };
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
